// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type AvgAggregationGroup struct {
	// Sum, presence, and histograms for each step.
	floatSums              []float64
	floatMeans             []float64
	floatCompensatingMeans []float64 // Mean, or "compensating value" for Kahan summation.
	floatPresent           []bool
	histograms             []*histogram.FloatHistogram
	histogramPointCount    int
	incrementalMeans       []bool // True after reverting to incremental calculation of the mean value.

	// Keeps track of how many series we have encountered thus far for the group at this point
	// CHECKME: This is necessary to do per point (instead of just counting the groups) as a series may have
	// many stale or non-existant values that are not added towards the count
	groupSeriesCounts []float64
}

func (g *AvgAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) (bool, error) {
	var err error

	if g.groupSeriesCounts == nil {
		g.groupSeriesCounts, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}
		g.groupSeriesCounts = g.groupSeriesCounts[:steps]

	}

	if len(data.Floats) > 0 && g.floatSums == nil {
		// First series with float values for this group, populate it.
		g.floatSums, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.floatCompensatingMeans, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.incrementalMeans, err = types.BoolSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.floatSums = g.floatSums[:steps]
		g.floatCompensatingMeans = g.floatCompensatingMeans[:steps]
		g.floatPresent = g.floatPresent[:steps]
		g.incrementalMeans = g.incrementalMeans[:steps]
	}

	if len(data.Histograms) > 0 && g.histograms == nil {
		// First series with histogram values for this group, populate it.
		g.histograms, err = types.HistogramSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}
		g.histograms = g.histograms[:steps]
	}

	haveMixedFloatsAndHistograms := false
	removeConflictingPoint := func(idx int64) {
		haveMixedFloatsAndHistograms = true
		g.floatPresent[idx] = false
		g.histograms[idx] = nil
		g.histogramPointCount--
	}

	for _, p := range data.Floats {
		idx := (p.T - start) / interval

		// Check that a NH doesn't already exist at this point. If both exist, the vector is removed.
		if g.histograms != nil && g.histograms[idx] != nil {
			removeConflictingPoint(idx)
			continue
		}

		g.groupSeriesCounts[idx]++
		if !g.floatPresent[idx] {
			// The first point is just taken as the value
			g.floatSums[idx] = p.F
			g.floatPresent[idx] = true
			continue
		}

		if !g.incrementalMeans[idx] {
			newV, newC := floats.KahanSumInc(p.F, g.floatSums[idx], g.floatCompensatingMeans[idx])
			if !math.IsInf(newV, 0) {
				// The sum doesn't overflow, so we propagate it to the
				// group struct and continue with the regular
				// calculation of the mean value.
				g.floatSums[idx], g.floatCompensatingMeans[idx] = newV, newC
				continue
			}
			// If we are here, we know that the sum _would_ overflow. So
			// instead of continuing to sum up, we revert to incremental
			// calculation of the mean value from here on.
			if g.floatMeans == nil {
				g.floatMeans, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
				if err != nil {
					return haveMixedFloatsAndHistograms, err
				}
				g.floatMeans = g.floatMeans[:steps]
			}
			g.incrementalMeans[idx] = true
			g.floatMeans[idx] = g.floatSums[idx] / (g.groupSeriesCounts[idx] - 1)
			g.floatCompensatingMeans[idx] /= g.groupSeriesCounts[idx] - 1
		}
		if math.IsInf(g.floatMeans[idx], 0) {
			if math.IsInf(p.F, 0) && (g.floatMeans[idx] > 0) == (p.F > 0) {
				// The `floatMean` and `s.F` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `floatMean` is correct
				// already.
				continue
			}
			if !math.IsInf(p.F, 0) && !math.IsNaN(p.F) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		currentMean := g.floatMeans[idx] + g.floatCompensatingMeans[idx]
		g.floatMeans[idx], g.floatCompensatingMeans[idx] = floats.KahanSumInc(
			p.F/g.groupSeriesCounts[idx]-currentMean/g.groupSeriesCounts[idx],
			g.floatMeans[idx],
			g.floatCompensatingMeans[idx],
		)
	}

	for _, p := range data.Histograms {
		idx := (p.T - start) / interval

		// Check that a float doesn't already exist at this point. If both exist, the vector is removed.
		if g.floatPresent != nil && g.floatPresent[idx] {
			removeConflictingPoint(idx)
			continue
		}

		g.groupSeriesCounts[idx]++
		if g.histograms[idx] == nil {
			// The first point is just taken as the value
			g.histograms[idx] = p.H.Copy()
			g.histogramPointCount++
			continue
		}

		if g.histograms[idx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		}

		left := p.H.Copy().Div(g.groupSeriesCounts[idx])
		right := g.histograms[idx].Copy().Div(g.groupSeriesCounts[idx])
		toAdd, err := left.Sub(right)
		if err != nil {
			// Unable to subtract histograms (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
			g.histograms[idx] = invalidCombinationOfHistograms
			g.histogramPointCount--

			if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotationFunc); err != nil {
				// Unknown error: we couldn't convert the error to an annotation. Give up.
				return false, err
			}
			continue
		}
		_, err = g.histograms[idx].Add(toAdd)
		if err != nil {
			// Unable to subtract histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
			g.histograms[idx] = invalidCombinationOfHistograms
			g.histogramPointCount--

			if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotationFunc); err != nil {
				// Unknown error: we couldn't convert the error to an annotation. Give up.
				return false, err
			}
			continue
		}
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return haveMixedFloatsAndHistograms, nil
}

func (g *AvgAggregationGroup) ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	var floatPoints []promql.FPoint
	var err error

	floatPointCount := 0
	for _, p := range g.floatPresent {
		if p {
			floatPointCount++
		}
	}

	if floatPointCount > 0 {
		floatPoints, err = types.FPointSlicePool.Get(floatPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, havePoint := range g.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				var f float64
				if g.incrementalMeans[i] {
					f = g.floatMeans[i] + g.floatCompensatingMeans[i]
				} else {
					f = (g.floatSums[i] + g.floatCompensatingMeans[i]) / g.groupSeriesCounts[i]
				}
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	var histogramPoints []promql.HPoint
	if g.histogramPointCount > 0 {
		histogramPoints, err = types.HPointSlicePool.Get(g.histogramPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, h := range g.histograms {
			if h != nil && h != invalidCombinationOfHistograms {
				t := start + int64(i)*interval
				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: g.histograms[i].Compact(0)})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatSums, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.floatMeans, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.floatCompensatingMeans, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.floatPresent, memoryConsumptionTracker)
	types.HistogramSlicePool.Put(g.histograms, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.incrementalMeans, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.groupSeriesCounts, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, nil
}
