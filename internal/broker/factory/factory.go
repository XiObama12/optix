// Package factory constructs concrete Broker implementations.
//
// It lives in its own subpackage (rather than in package broker) so that
// concrete brokers (ibkr, yfinance, longbridge) can depend on `broker` for
// shared types like ErrExpiryNotAvailable without creating an import cycle.
package factory

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/IS908/optix/internal/broker"
	"github.com/IS908/optix/internal/broker/ibkr"
	"github.com/IS908/optix/internal/broker/longbridge"
	"github.com/IS908/optix/internal/broker/yfinance"
	"github.com/IS908/optix/pkg/model"
)

// defaultAutoOrder is the --source auto fallback chain, tried left to right.
// The first source that connects serves every later call.
//
// Override with OPTIX_AUTO_SOURCES, e.g. "longbridge,yfinance" to skip a
// local IB Gateway entirely.
const defaultAutoOrder = "ibkr,longbridge,yfinance"

func autoOrder() []string {
	raw := strings.TrimSpace(os.Getenv("OPTIX_AUTO_SOURCES"))
	if raw == "" {
		raw = defaultAutoOrder
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// NewWithFallback builds the auto source chain. FallbackBroker supports only
// two levels, so three or more sources nest: the leftmost becomes the primary
// and the rest fold into the fallback.
//
// pythonBin is the interpreter used by subprocess-backed sources (yfinance,
// longbridge). Empty means "python3".
func NewWithFallback(ibCfg ibkr.Config, pythonBin string) *broker.FallbackBroker {
	order := autoOrder()

	var chain broker.Broker
	// Fold right-to-left so the first listed source ends up outermost.
	for i := len(order) - 1; i >= 0; i-- {
		var next broker.Broker
		switch order[i] {
		case "ibkr":
			cfg := ibCfg
			if chain != nil {
				cfg.OptionSpotFallback = spotFallback(chain)
			}
			next = ibkr.New(cfg)
		case "longbridge":
			next = longbridge.New(longbridge.Config{PythonBin: pythonBin})
		case "yfinance":
			next = yfinance.New(yfinance.Config{PythonBin: pythonBin})
		default:
			continue
		}

		if chain == nil {
			chain = next
		} else {
			chain = broker.NewFallbackBroker(next, chain)
		}
	}

	// Unrecognised OPTIX_AUTO_SOURCES would leave chain nil; hand back a
	// working Yahoo broker rather than a nil dereference.
	if chain == nil {
		chain = yfinance.New(yfinance.Config{PythonBin: pythonBin})
	}

	if fb, ok := chain.(*broker.FallbackBroker); ok {
		return fb
	}
	return broker.NewFallbackBroker(chain, chain)
}

// spotFallback adapts any Broker to the hook IBKR uses to price an option's
// underlying when IBKR itself cannot quote it.
func spotFallback(b broker.Broker) func(context.Context, string) (*model.StockQuote, error) {
	return func(ctx context.Context, symbol string) (*model.StockQuote, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return b.GetQuote(ctx, symbol)
	}
}
