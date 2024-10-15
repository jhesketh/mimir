// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"math"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var Abs = NewFloatTransformationDropHistogramsFunction(math.Abs)
var Acos = NewFloatTransformationDropHistogramsFunction(math.Acos)
var Acosh = NewFloatTransformationDropHistogramsFunction(math.Acosh)
var Asin = NewFloatTransformationDropHistogramsFunction(math.Asin)
var Asinh = NewFloatTransformationDropHistogramsFunction(math.Asinh)
var Atan = NewFloatTransformationDropHistogramsFunction(math.Atan)
var Atanh = NewFloatTransformationDropHistogramsFunction(math.Atanh)
var Ceil = NewFloatTransformationDropHistogramsFunction(math.Ceil)
var Cos = NewFloatTransformationDropHistogramsFunction(math.Cos)
var Cosh = NewFloatTransformationDropHistogramsFunction(math.Cosh)
var Exp = NewFloatTransformationDropHistogramsFunction(math.Exp)
var Floor = NewFloatTransformationDropHistogramsFunction(math.Floor)
var Ln = NewFloatTransformationDropHistogramsFunction(math.Log)
var Log10 = NewFloatTransformationDropHistogramsFunction(math.Log10)
var Log2 = NewFloatTransformationDropHistogramsFunction(math.Log2)
var Sin = NewFloatTransformationDropHistogramsFunction(math.Sin)
var Sinh = NewFloatTransformationDropHistogramsFunction(math.Sinh)
var Sqrt = NewFloatTransformationDropHistogramsFunction(math.Sqrt)
var Tan = NewFloatTransformationDropHistogramsFunction(math.Tan)
var Tanh = NewFloatTransformationDropHistogramsFunction(math.Tanh)

var Deg = NewFloatTransformationDropHistogramsFunction(func(f float64) float64 {
	return f * 180 / math.Pi
})

var Rad = NewFloatTransformationDropHistogramsFunction(func(f float64) float64 {
	return f * math.Pi / 180
})

var Sgn = NewFloatTransformationDropHistogramsFunction(func(f float64) float64 {
	if f < 0 {
		return -1
	}

	if f > 0 {
		return 1
	}

	// This behaviour is undocumented, but if f is +/- NaN, Prometheus' engine returns that value.
	// Otherwise, if the value is 0, we should return 0.
	return f
})

type UnaryNegationFunction struct{}

func (f *UnaryNegationFunction) Func(seriesData types.InstantVectorSeriesData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	for i := range seriesData.Floats {
		seriesData.Floats[i].F = -seriesData.Floats[i].F
	}

	for i := range seriesData.Histograms {
		seriesData.Histograms[i].H.Mul(-1) // Mul modifies the histogram in-place, so we don't need to do anything with the result here.
	}

	return seriesData, nil
}

func (f *UnaryNegationFunction) Close() {
}

type ClampFunction struct {
	minValues                types.ScalarData
	maxValues                types.ScalarData
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
}

func NewClampFunction(memoryConsumptionTracker *limiting.MemoryConsumptionTracker, min, max types.ScalarOperator) (*ClampFunction, error) {
	ctx := context.Background()
	minValues, err := min.GetValues(ctx)
	if err != nil {
		return nil, err
	}
	maxValues, err := max.GetValues(ctx)
	if err != nil {
		return nil, err
	}
	return &ClampFunction{
		minValues:                minValues,
		maxValues:                maxValues,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}, nil
}

func (f *ClampFunction) Func(seriesData types.InstantVectorSeriesData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	for step, data := range seriesData.Floats {
		minVal := f.minValues.Samples[step].F
		maxVal := f.maxValues.Samples[step].F
		// We reuse the existing FPoint slice in place
		seriesData.Floats[step].F = math.Max(minVal, math.Min(maxVal, data.F))
	}

	return seriesData, nil
}

func (f *ClampFunction) Close() {
	types.FPointSlicePool.Put(f.minValues.Samples, f.memoryConsumptionTracker)
	types.FPointSlicePool.Put(f.maxValues.Samples, f.memoryConsumptionTracker)
}
