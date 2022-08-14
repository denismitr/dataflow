package gs

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

var ErrReducerRequired = errors.New("reducer function is required")

type (
	orderedMapReduceSink[K constraints.Ordered, V, R any] func(ctx context.Context, flow *flow[K, V]) (R, error)

	ReducableOrderedMapStream[K constraints.Ordered, V, R any] struct {
		OrderedMapStream[K, V]
		reduceSink orderedMapReduceSink[K, V, R]
	}
)

func NewReducableOrderedMapStream[K constraints.Ordered, V, R any](
	om *OrderedMap[K, V],
	options ...FlowOption,
) *ReducableOrderedMapStream[K, V, R] {
	fc := flowControl{
		concurrency: DefaultConcurrency,
	}

	for _, o := range options {
		o(&fc)
	}

	return &ReducableOrderedMapStream[K, V, R]{
		OrderedMapStream: OrderedMapStream[K, V]{
			om:          om,
			concurrency: fc.concurrency,
		},
	}
}

func (s *ReducableOrderedMapStream[K, V, R]) Reduce(
	reducer OrderedMapReduceFn[K, V, R],
) *ReducableOrderedMapStream[K, V, R] {
	f := func(ctx context.Context, flow *flow[K, V]) (R, error) {
		var result R
		for {
			select {
			case <-ctx.Done():
				return getZero[R](), errors.Wrap(ctx.Err(), "reduce interrupted")
			case pair, ok := <-flow.ch:
				if ok {
					result = reducer(result, pair.Key, pair.Value, pair.Order)
				} else {
					return result, nil
				}
			}
		}
	}

	s.reduceSink = f

	return s
}

func (s *ReducableOrderedMapStream[K, V, R]) PipeToResult(ctx context.Context) (R, error) {
	if s.reduceSink == nil {
		return getZero[R](), ErrReducerRequired
	}

	outFlow, err := s.run(ctx)
	if err != nil {
		return getZero[R](), errors.Wrap(err, "reducable ordered map stream failed")
	}

	result, err := s.reduceSink(ctx, outFlow)
	if err != nil {
		return getZero[R](), errors.Wrap(err, "reducable ordered map stream failed")
	}

	return result, nil
}
