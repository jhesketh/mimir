// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorFunctionOperatorFactory func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error)

func SimpleFunctionFactory(name string, seriesDataFunc functions.InstantVectorFunction) InstantVectorFunctionOperatorFactory {
	// Simple Function Factory is for functions that have exactly 1 argument and drop the series name.
	return func(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		return &operators.FunctionOverInstantVector{
			Inner: inner,
			Pool:  pool,

			MetadataFunc:   functions.DropSeriesName,
			SeriesDataFunc: seriesDataFunc,
		}, nil
	}
}

func rateFunction(args []types.Operator, pool *pooling.LimitingPool) (types.InstantVectorOperator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for rate, got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a range vector argument for rate, got %T", args[0])
	}

	return &operators.FunctionOverRangeVector{
		Inner: inner,
		Pool:  pool,
	}, nil
}

var _ InstantVectorFunctionOperatorFactory = rateFunction

// These functions return an instant-vector.
var instantVectorFunctions = map[string]InstantVectorFunctionOperatorFactory{
	"acos":            SimpleFunctionFactory("acos", functions.Acos),
	"histogram_count": SimpleFunctionFactory("histogram_count", functions.HistogramCount),
	"histogram_sum":   SimpleFunctionFactory("histogram_sum", functions.HistogramSum),
	"rate":            rateFunction,
}
