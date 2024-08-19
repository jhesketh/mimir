// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AggregationFunction accumulates series that have been grouped together and computes the output series data.
type AggregationFunction interface {
	// AccumulateSeries takes in a series as part of the group
	AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) error
	// ComputeOutputSeries does any final calculations and returns the grouped series data
	ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error)
}
