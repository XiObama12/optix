// Package longbridge implements broker.Broker against Longbridge's market
// data, using the same subprocess pattern as the yfinance adapter: the Go
// side shells out to an embedded Python script that talks to the Longbridge
// MCP endpoint.
//
// Why MCP rather than the `longport` SDK: the SDK signs requests with an
// app_key/app_secret pair which is not present on this machine. The MCP
// endpoint authenticates with a single bearer JWT that already exists and is
// in active use, so it needs no new credentials.
//
// Differences from the IBKR source worth knowing:
//   - Bid/ask come from a separate `depth` round trip per contract, so the
//     adapter only enriches the strikes nearest the money (see
//     LONG_BRIDGE_DEPTH_STRIKES in fetcher.py). Farther strikes report 0/0.
//   - Data is real-time for US equities, unlike yfinance's 15-minute delay.
package longbridge

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/IS908/optix/internal/broker"
	"github.com/IS908/optix/pkg/model"
)

//go:embed fetcher.py
var fetcherSource []byte

var (
	fetcherOnce sync.Once
	fetcherPath string
	fetcherErr  error
)

// resolveFetcher writes the embedded script to a stable per-build temp file.
// Mirrors the yfinance adapter so both sources behave identically under
// repeated invocation and concurrent calls.
func resolveFetcher() (string, error) {
	fetcherOnce.Do(func() {
		sum := sha256.Sum256(fetcherSource)
		name := fmt.Sprintf("optix-lb-fetcher-%s.py", hex.EncodeToString(sum[:8]))
		dir := os.TempDir()
		path := filepath.Join(dir, name)

		if existing, err := os.ReadFile(path); err == nil && len(existing) == len(fetcherSource) {
			fetcherPath = path
			return
		}

		tmp, err := os.CreateTemp(dir, name+".*")
		if err != nil {
			fetcherErr = fmt.Errorf("longbridge: stage fetcher.py: %w", err)
			return
		}
		if _, err := tmp.Write(fetcherSource); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			fetcherErr = fmt.Errorf("longbridge: write fetcher.py: %w", err)
			return
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmp.Name())
			fetcherErr = fmt.Errorf("longbridge: close fetcher.py: %w", err)
			return
		}
		if err := os.Rename(tmp.Name(), path); err != nil {
			os.Remove(tmp.Name())
			fetcherErr = fmt.Errorf("longbridge: rename fetcher.py: %w", err)
			return
		}
		fetcherPath = path
	})
	return fetcherPath, fetcherErr
}

// RunFetcher executes the embedded fetcher with the given args and returns
// its stdout. Exported for reuse by other packages that need raw Longbridge
// reads.
func RunFetcher(ctx context.Context, pythonBin string, args ...string) ([]byte, error) {
	if pythonBin == "" {
		pythonBin = "python3"
	}
	fetcher, err := resolveFetcher()
	if err != nil {
		return nil, err
	}

	cmdArgs := append([]string{fetcher}, args...)
	cmd := exec.CommandContext(ctx, pythonBin, cmdArgs...)
	// Close pipes 5s after ctx expires so an orphaned grandchild cannot hold
	// the write end open and block Output() indefinitely.
	cmd.WaitDelay = 5 * time.Second

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%s: %s", err, string(exitErr.Stderr))
		}
		return nil, err
	}
	return out, nil
}

// Config configures the Longbridge data source.
type Config struct {
	// PythonBin is the interpreter used to run the fetcher.
	// Empty means "python3".
	PythonBin string
}

// Client implements broker.Broker over Longbridge market data.
type Client struct {
	cfg       Config
	connected bool
}

// New builds a Longbridge client. It never fails here; connectivity problems
// surface on the first call so the FallbackBroker can react.
func New(cfg Config) *Client {
	if cfg.PythonBin == "" {
		cfg.PythonBin = "python3"
	}
	return &Client{cfg: cfg}
}

// Connect is a no-op: the MCP endpoint is stateless HTTP, so there is no
// session to establish. It marks the client ready so FallbackBroker treats
// this source as the active one.
func (c *Client) Connect(ctx context.Context) error {
	c.connected = true
	return nil
}

func (c *Client) Disconnect() error {
	c.connected = false
	return nil
}

func (c *Client) IsConnected() bool { return c.connected }

// Ping issues a real round trip so health checks detect an unreachable proxy
// or an expired token instead of reporting a false healthy state.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.runFetcher(ctx, "quote", "AAPL")
	return err
}

func (c *Client) runFetcher(ctx context.Context, args ...string) ([]byte, error) {
	return RunFetcher(ctx, c.cfg.PythonBin, args...)
}

// ── Quote ───────────────────────────────────────────────────────────────────

func (c *Client) GetQuote(ctx context.Context, symbol string) (*model.StockQuote, error) {
	out, err := c.runFetcher(ctx, "quote", symbol)
	if err != nil {
		return nil, fmt.Errorf("longbridge quote %s: %w", symbol, err)
	}

	var raw struct {
		Symbol    string  `json:"symbol"`
		Last      float64 `json:"last"`
		Bid       float64 `json:"bid"`
		Ask       float64 `json:"ask"`
		Volume    int64   `json:"volume"`
		Change    float64 `json:"change"`
		ChangePct float64 `json:"changePct"`
		High      float64 `json:"high"`
		Low       float64 `json:"low"`
		Open      float64 `json:"open"`
		Close     float64 `json:"close"`
		High52W   float64 `json:"high52w"`
		Low52W    float64 `json:"low52w"`
		AvgVolume float64 `json:"avgVolume"`
		Timestamp string  `json:"timestamp"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("parse longbridge quote: %w", err)
	}

	ts, _ := time.Parse(time.RFC3339Nano, raw.Timestamp)
	if ts.IsZero() {
		ts, _ = time.Parse(time.RFC3339, raw.Timestamp)
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	return &model.StockQuote{
		Symbol:        raw.Symbol,
		Last:          raw.Last,
		Bid:           raw.Bid,
		Ask:           raw.Ask,
		Volume:        raw.Volume,
		Change:        raw.Change,
		ChangePct:     raw.ChangePct,
		High:          raw.High,
		Low:           raw.Low,
		Open:          raw.Open,
		Close:         raw.Close,
		High52W:       raw.High52W,
		Low52W:        raw.Low52W,
		AvgVolume:     raw.AvgVolume,
		Timestamp:     ts,
		MarketSession: model.USMarketSession(time.Now()),
	}, nil
}

// ── Historical bars ─────────────────────────────────────────────────────────

func (c *Client) GetHistoricalBars(ctx context.Context, symbol, timeframe, startDate, endDate string) ([]model.OHLCV, error) {
	// Longbridge returns the most recent N bars; derive N from the window.
	days := 365
	if startDate != "" {
		if t, err := time.Parse("2006-01-02", startDate); err == nil {
			if d := int(time.Since(t).Hours() / 24); d > 0 {
				days = d + 5
			}
		}
	}

	out, err := c.runFetcher(ctx, "bars", symbol, timeframe, fmt.Sprint(days))
	if err != nil {
		return nil, fmt.Errorf("longbridge bars %s: %w", symbol, err)
	}

	var raw []struct {
		Timestamp string  `json:"timestamp"`
		Open      float64 `json:"open"`
		High      float64 `json:"high"`
		Low       float64 `json:"low"`
		Close     float64 `json:"close"`
		Volume    int64   `json:"volume"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("parse longbridge bars: %w", err)
	}

	bars := make([]model.OHLCV, 0, len(raw))
	for _, b := range raw {
		ts, _ := time.Parse(time.RFC3339Nano, b.Timestamp)
		if ts.IsZero() {
			ts, _ = time.Parse(time.RFC3339, b.Timestamp)
		}
		bars = append(bars, model.OHLCV{
			Timestamp: ts,
			Open:      b.Open,
			High:      b.High,
			Low:       b.Low,
			Close:     b.Close,
			Volume:    b.Volume,
		})
	}
	return bars, nil
}

// ── Option chain ────────────────────────────────────────────────────────────

type rawOptionRow struct {
	Strike       float64 `json:"strike"`
	Bid          float64 `json:"bid"`
	Ask          float64 `json:"ask"`
	Last         float64 `json:"last"`
	Volume       int64   `json:"volume"`
	OpenInterest int32   `json:"openInterest"`
	IV           float64 `json:"iv"`
}

func (r rawOptionRow) toQuote(underlying, expiration string, kind model.OptionType) model.OptionQuote {
	mid := 0.0
	if r.Bid > 0 && r.Ask > 0 {
		mid = (r.Bid + r.Ask) / 2
	}
	return model.OptionQuote{
		Underlying:        underlying,
		Expiration:        expiration,
		Strike:            r.Strike,
		OptionType:        kind,
		Last:              r.Last,
		Bid:               r.Bid,
		Ask:               r.Ask,
		Mid:               mid,
		Volume:            r.Volume,
		OpenInterest:      r.OpenInterest,
		ImpliedVolatility: r.IV,
	}
}

func (c *Client) GetOptionChain(ctx context.Context, underlying string, expiration string) (*model.OptionChain, error) {
	args := []string{"option_chain", underlying}
	if expiration != "" {
		args = append(args, expiration)
	}
	out, err := c.runFetcher(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("longbridge option_chain %s: %w", underlying, err)
	}

	var raw struct {
		Underlying  string `json:"underlying"`
		Expirations []struct {
			Expiration string         `json:"expiration"`
			Calls      []rawOptionRow `json:"calls"`
			Puts       []rawOptionRow `json:"puts"`
		} `json:"expirations"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("parse longbridge option_chain: %w", err)
	}

	want := strings.ReplaceAll(expiration, "-", "")
	chain := &model.OptionChain{Underlying: raw.Underlying}
	for _, e := range raw.Expirations {
		exp := model.OptionChainExpiry{Expiration: e.Expiration}
		for _, c := range e.Calls {
			exp.Calls = append(exp.Calls, c.toQuote(raw.Underlying, e.Expiration, model.OptionTypeCall))
		}
		for _, p := range e.Puts {
			exp.Puts = append(exp.Puts, p.toQuote(raw.Underlying, e.Expiration, model.OptionTypePut))
		}
		chain.Expirations = append(chain.Expirations, exp)
	}

	if want != "" {
		var matched *model.OptionChainExpiry
		for i := range chain.Expirations {
			e := &chain.Expirations[i]
			if e.Expiration == want && (len(e.Calls) > 0 || len(e.Puts) > 0) {
				matched = e
				break
			}
		}
		if matched == nil {
			avail := make([]string, 0, len(chain.Expirations))
			for _, e := range chain.Expirations {
				avail = append(avail, e.Expiration)
			}
			return nil, &broker.ErrExpiryNotAvailable{
				Underlying: underlying,
				Requested:  want,
				Available:  avail,
			}
		}
		chain.Expirations = []model.OptionChainExpiry{*matched}
	}

	return chain, nil
}

// OIFetcher — Longbridge returns per-contract open interest inline with the
// chain, so this is the same call as GetOptionChain.
func (c *Client) GetOptionChainWithOI(ctx context.Context, underlying string, expiration string) (*model.OptionChain, error) {
	return c.GetOptionChain(ctx, underlying, expiration)
}

var _ broker.Broker = (*Client)(nil)
var _ broker.Pinger = (*Client)(nil)
var _ broker.OIFetcher = (*Client)(nil)

// SourceName identifies this source in the data-source banner.
func (c *Client) SourceName() string { return "Longbridge (real-time)" }
