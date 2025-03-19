// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/retry_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	fmt "fmt"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

func TestRetry(t *testing.T) {
	var try atomic.Int32

	errBadRequest := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusBadRequest,
		Body: []byte("Bad Request"),
	})
	errUnprocessable := apierror.New(apierror.TypeBadData, "invalid expression type \"range vector\" for range query, must be Scalar or instant Vector\"")
	errInternal := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusInternalServerError,
		Body: []byte("Internal Server Error"),
	})

	for _, tc := range []struct {
		name            string
		handler         MetricsQueryHandler
		resp            Response
		err             error
		expectedRetries int
	}{
		{
			name:            "retry failures",
			expectedRetries: 4,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
				if try.Inc() == 5 {
					return responseWithFinalizer{response: &PrometheusResponse{Status: "Hello World"}}, nil
				}
				return responseWithFinalizer{}, fmt.Errorf("fail")
			}),
			resp: &PrometheusResponse{Status: "Hello World"},
		},
		{
			name:            "don't retry 400s",
			expectedRetries: 0,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
				return responseWithFinalizer{}, errBadRequest
			}),
			err: errBadRequest,
		},
		{
			name:            "don't retry bad-data",
			expectedRetries: 0,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
				return responseWithFinalizer{}, errUnprocessable
			}),
			err: errUnprocessable,
		},
		{
			name:            "retry 500s",
			expectedRetries: 5,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
				return responseWithFinalizer{}, errInternal
			}),
			err: errInternal,
		},
		{
			name:            "last error",
			expectedRetries: 4,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
				if try.Inc() == 5 {
					return responseWithFinalizer{}, errBadRequest
				}
				return responseWithFinalizer{}, errInternal
			}),
			err: errBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			try.Store(0)
			mockMetrics := mockRetryMetrics{}
			h := newRetryMiddleware(log.NewNopLogger(), 5, &mockMetrics).Wrap(tc.handler)
			resp, err := h.Do(context.Background(), nil)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
			require.Equal(t, float64(tc.expectedRetries), mockMetrics.retries)
		})
	}
}

type mockRetryMetrics struct {
	retries float64
}

func (c *mockRetryMetrics) Observe(v float64) {
	c.retries = v
}

func Test_RetryMiddlewareCancel(t *testing.T) {
	var try atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
			try.Inc()
			return responseWithFinalizer{}, ctx.Err()
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(0), try.Load())
	require.Equal(t, ctx.Err(), err)

	ctx, cancel = context.WithCancel(context.Background())
	_, err = newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(context.Context, MetricsQueryRequest) (responseWithFinalizer, error) {
			try.Inc()
			cancel()
			return responseWithFinalizer{}, errors.New("failed")
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(1), try.Load())
	require.Equal(t, ctx.Err(), err)
}
