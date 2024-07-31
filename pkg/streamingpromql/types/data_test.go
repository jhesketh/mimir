// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestInstantVectorSeriesDataIterator(t *testing.T) {
	type expected struct {
		T       int64
		F       float64
		H       *histogram.FloatHistogram
		HasNext bool
	}
	type testCase struct {
		name     string
		data     *InstantVectorSeriesData
		expected []expected
	}

	testCases := []testCase{
		{
			name: "floats only",
			data: &InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, true},
				{2000, 2.2, nil, true},
				{3000, 3.3, nil, true},
				{0, 0, nil, false},
			},
		},
		{
			name: "histograms only",
			data: &InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{}},
					{T: 2500, H: &histogram.FloatHistogram{}},
				},
			},
			expected: []expected{
				{1500, 0, &histogram.FloatHistogram{}, true},
				{2500, 0, &histogram.FloatHistogram{}, true},
				{0, 0, nil, false},
			},
		},
		{
			name: "mixed data",
			data: &InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
					{T: 4000, F: 4.4},
					{T: 5000, F: 5.5},
				},
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{}},
					{T: 2500, H: &histogram.FloatHistogram{}},
					{T: 5500, H: &histogram.FloatHistogram{}},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, true},
				{1500, 0, &histogram.FloatHistogram{}, true},
				{2000, 2.2, nil, true},
				{2500, 0, &histogram.FloatHistogram{}, true},
				{3000, 3.3, nil, true},
				{4000, 4.4, nil, true},
				{5000, 5.5, nil, true},
				{5500, 0, &histogram.FloatHistogram{}, true},
				{0, 0, nil, false},
			},
		},
		{
			name: "empty data",
			data: &InstantVectorSeriesData{},
			expected: []expected{
				{0, 0, nil, false},
			},
		},
		{
			name: "multiple next calls after exhaustion",
			data: &InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, true},
				{0, 0, nil, false},
				{0, 0, nil, false},
				{0, 0, nil, false},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter := NewInstantVectorSeriesDataIterator(tc.data)

			for _, exp := range tc.expected {
				timestamp, floatVal, hist, hasNext := iter.Next()
				require.Equal(t, exp.T, timestamp)
				require.Equal(t, exp.F, floatVal)
				require.Equal(t, exp.H, hist)
				require.Equal(t, exp.HasNext, hasNext)
			}
		})
	}
}
