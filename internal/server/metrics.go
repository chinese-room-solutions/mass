package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/chinese-room-solutions/mass/internal/metrics"
)

// NewMetricsInterceptor returns a ConnectRPC unary interceptor that records request metrics.
func NewMetricsInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			method := req.Spec().Procedure
			metrics.RequestReceived(method)
			start := time.Now()
			resp, err := next(ctx, req)
			metrics.RequestDuration(method, time.Since(start).Seconds())
			return resp, err
		}
	}
}
