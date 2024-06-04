// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Scalar struct {
	Expr *parser.NumberLiteral
}

var _ types.Operator = &Scalar{}

func (s *Scalar) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	return nil, fmt.Errorf("SeriesMetadata should not be called for Scalar")
}

func (s *Scalar) GetFloat() float64 {
	return s.Expr.Val
}

func (s *Scalar) Close() {}
