package exchanges

import (
	"context"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func sendError(ctx context.Context, errs chan<- error, err error) {
	select {
	case errs <- err:
	case <-ctx.Done():
	default:
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sendTrade(ctx context.Context, trades chan<- quanttick.TradeEvent, trade quanttick.TradeEvent) error {
	select {
	case trades <- trade:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
