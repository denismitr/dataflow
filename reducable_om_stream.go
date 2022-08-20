package gs

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

var ErrReducerRequired = errors.New("reducer function is required")

type (
	keyValueReduceSink[K constraints.Ordered, V, R any] func(ctx context.Context, flow *flow[K, V]) (R, error)

	ReducableKeyValueStream[K constraints.Ordered, V, R any] struct {
		oms *OrderedMapStream[K, V]
	}
)

func NewReducableOrderedMapStream[K constraints.Ordered, V, R any](
	om *OrderedMap[K, V],
	options ...FlowOption,
) *ReducableKeyValueStream[K, V, R] {
	fc := flowControl{
		concurrency: DefaultConcurrency,
	}

	for _, o := range options {
		o(&fc)
	}

	return &ReducableKeyValueStream[K, V, R]{
		oms: &OrderedMapStream[K, V]{
			om: om,
			fc: fc,
		},
	}
}

func (s *ReducableKeyValueStream[K, V, R]) Map(
	mapper MapKeyValueFn[K, V],
	options ...FlowOption,
) *ReducableKeyValueStream[K, V, R] {
	s.oms.Map(mapper, options...)
	return s
}

func (s *ReducableKeyValueStream[K, V, R]) Filter(
	filter FilterKeyValueFn[K, V],
	options ...FlowOption,
) *ReducableKeyValueStream[K, V, R] {
	s.oms.Filter(filter, options...)
	return s
}

func (s *ReducableKeyValueStream[K, V, R]) Reduce(
	ctx context.Context,
	reducer ReduceKeyValueFn[K, V, R],
) (R, error) {
	reduceSink := func(ctx context.Context, flow *flow[K, V]) (R, error) {
		var result R
		for {
			select {
			case <-ctx.Done():
				return getZero[R](), errors.Wrap(ctx.Err(), "reduce interrupted")
			case pair, ok := <-flow.ch:
				if ok {
					result = reducer(result, pair.Key, pair.Value)
				} else {
					return result, nil
				}
			}
		}
	}

	outFlow, err := s.oms.run(ctx)
	if err != nil {
		return getZero[R](), errors.Wrap(err, "reduce ordered map stream failed")
	}

	result, err := reduceSink(ctx, outFlow)
	if err != nil {
		return getZero[R](), errors.Wrap(err, "reduce ordered map stream failed")
	}

	return result, nil
}
