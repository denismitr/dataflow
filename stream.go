package gs

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type (
	OrderedMapFilterFn[K constraints.Ordered, V any]    func(key K, value V, order int) bool
	OrderedMapMapperFn[K constraints.Ordered, V any]    func(key K, value V, order int) V
	OrderedMapForEachFn[K constraints.Ordered, V any]   func(key K, value V, order int)
	OrderedMapReduceFn[K constraints.Ordered, V, R any] func(carry R, key K, value V, order int) R
	OrderedMapFirstFn[K constraints.Ordered, V any]     func(key K, value V, order int) (bool, error)

	orderedPipe[K constraints.Ordered, V any] func(ctx context.Context, flow *flow[K, V]) *flow[K, V]
)

type OrderedMapStream[K constraints.Ordered, V any] struct {
	om *OrderedMap[K, V]
	fc flowControl

	functions []orderedPipe[K, V]
}

func NewOrderedMapStream[K constraints.Ordered, V any](
	om *OrderedMap[K, V],
	options ...FlowOption,
) *OrderedMapStream[K, V] {
	fc := flowControl{
		concurrency: DefaultConcurrency,
	}

	for _, o := range options {
		o(&fc)
	}

	return &OrderedMapStream[K, V]{
		om: om,
		fc: fc, // fixme: assign flowControl
	}
}

func (s *OrderedMapStream[K, V]) Filter(
	filter OrderedMapFilterFn[K, V],
	options ...FlowOption,
) *OrderedMapStream[K, V] {
	localFc := flowControl{
		concurrency: s.fc.concurrency,
	}

	for _, o := range options {
		o(&localFc)
	}

	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](localFc.concurrency)

		var wg sync.WaitGroup
		wg.Add(int(localFc.concurrency))

		for i := 0; i < int(localFc.concurrency); i++ {
			go func() {
				defer wg.Done()

				for {
					select {
					case <-out.stop:
						flow.stop <- struct{}{}
						return
					case oPair, ok := <-flow.ch:
						if ok {
							if v := filter(oPair.Key, oPair.Value, oPair.Order); v {
								out.ch <- OrderedPair[K, V]{
									Key:   oPair.Key,
									Value: oPair.Value,
									Order: oPair.Order,
								}
							}
						} else {
							flow.stop <- struct{}{}
							return
						}
					case <-ctx.Done():
						flow.stop <- struct{}{}
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out.ch)
		}()

		return out
	}

	s.functions = append(s.functions, f)
	return s
}

func (s *OrderedMapStream[K, V]) Map(
	mapper OrderedMapMapperFn[K, V],
	options ...FlowOption,
) *OrderedMapStream[K, V] {
	localFc := flowControl{
		concurrency: s.fc.concurrency,
	}

	for _, o := range options {
		o(&localFc)
	}

	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](localFc.concurrency)

		var wg sync.WaitGroup
		wg.Add(int(localFc.concurrency))

		for i := 0; i < int(localFc.concurrency); i++ {
			go func() {
				defer wg.Done()

				for {
					select {
					case <-out.stop:
						flow.stop <- struct{}{}
						return
					case oPair, ok := <-flow.ch:
						if ok {
							newValue := mapper(oPair.Key, oPair.Value, oPair.Order)
							out.ch <- OrderedPair[K, V]{
								Key:   oPair.Key,
								Value: newValue,
								Order: oPair.Order,
							}
						} else {
							return
						}
					case <-ctx.Done():
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out.ch)
		}()

		return out
	}

	s.functions = append(s.functions, f)
	return s
}

func (s *OrderedMapStream[K, V]) ForEach(effector OrderedMapForEachFn[K, V], options ...FlowOption) *OrderedMapStream[K, V] {
	localFc := flowControl{
		concurrency: s.fc.concurrency,
	}

	for _, o := range options {
		o(&localFc)
	}

	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](localFc.concurrency)
		var wg sync.WaitGroup
		wg.Add(int(localFc.concurrency))

		for i := 0; i < int(localFc.concurrency); i++ {
			go func() {
				defer wg.Done()

				for {
					select {
					case <-out.stop:
						flow.stop <- struct{}{}
						return
					case pair, ok := <-flow.ch:
						if ok {
							effector(pair.Key, pair.Value, pair.Order)
							out.ch <- OrderedPair[K, V]{
								Key:   pair.Key,
								Value: pair.Value,
								Order: pair.Order,
							}
						} else {
							return
						}
					case <-ctx.Done():
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out.ch)
		}()

		return out
	}

	s.functions = append(s.functions, f)
	return s
}

// Take n items from stream
// works only in single threaded mode
func (s *OrderedMapStream[K, V]) Take(n int) *OrderedMapStream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](DefaultConcurrency)

		go func() {
			defer close(out.ch)

			taken := 0
			for {
				select {
				case pair, ok := <-flow.ch:
					if ok {
						out.ch <- OrderedPair[K, V]{
							Key:   pair.Key,
							Value: pair.Value,
							Order: pair.Order,
						}
						taken++
					} else {
						flow.stop <- struct{}{}
						return
					}
				case <-out.stop:
					flow.stop <- struct{}{}
					return
				case <-ctx.Done():
					return
				}

				if taken >= n {
					return
				}
			}
		}()

		return out
	}

	s.functions = append(s.functions, f)
	return s
}

func (s *OrderedMapStream[K, V]) First(
	baseCtx context.Context,
	matcher OrderedMapFirstFn[K, V],
	options ...FlowOption,
) (OrderedPair[K, V], error) {
	outFlow, err := s.run(baseCtx)
	if err != nil {
		return getZero[OrderedPair[K, V]](), errors.Wrap(err, "failed to get the first value")
	}

	localFc := flowControl{
		concurrency: s.fc.concurrency,
	}

	for _, o := range options {
		o(&localFc)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	resultCh := make(chan OrderedPair[K, V], localFc.concurrency)
	errCh := make(chan error, localFc.concurrency)

	var wg sync.WaitGroup
	wg.Add(int(localFc.concurrency))

	for i := 0; i < int(localFc.concurrency); i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case oPair, ok := <-outFlow.ch:
					if ok {
						match, err := matcher(oPair.Key, oPair.Value, oPair.Order)
						if err != nil {
							errCh <- err
						}
						if match {
							resultCh <- oPair
						}
					} else {
						return
					}
				}
			}
		}()
	}

	// in case nothing is ever found
	go func() {
		wg.Wait()
		cancel()
	}()

	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return getZero[OrderedPair[K, V]](), errors.Wrap(err, "no match found")
			}
		case <-ctx.Done():
			return getZero[OrderedPair[K, V]](), errors.Wrap(ctx.Err(), "no match found")
		case result, ok := <-resultCh:
			if ok {
				return result, nil
			}
		}
	}
}

// PipeToOrderedMap - runs the pipe of effectors and returns the resulting
// ordered map or error
func (s *OrderedMapStream[K, V]) PipeToOrderedMap(ctx context.Context) (*OrderedMap[K, V], error) {
	outFlow, err := s.run(ctx)
	if err != nil {
		return nil, err
	}

	var pairSlice OrderedPairs[K, V]

resultLoop:
	for {
		select {
		case result, ok := <-outFlow.ch:
			if ok {
				pairSlice = append(pairSlice, result)
			} else {
				break resultLoop
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	sort.Sort(pairSlice)

	// todo: sort
	return fromOrderedPairs(pairSlice), nil
}

func (s *OrderedMapStream[K, V]) run(baseCtx context.Context) (*flow[K, V], error) {
	if s.fc.concurrency < 1 {
		return nil, errors.Wrapf(ErrInvalidConcurrency, "should be greater than 1, got %d", s.fc.concurrency)
	}

	inFlow := newFlow[K, V](s.fc.concurrency)

	go func() {
		ctx, cancel := context.WithCancel(baseCtx)

		defer func() {
			cancel()
			close(inFlow.ch)
		}()

		for p := range s.om.pairs(ctx) {
			select {
			case inFlow.ch <- p:
			case <-inFlow.stop:
				return
			case <-baseCtx.Done():
				return
			}
		}
	}()

	outFlow := s.launchActionOnFlow(baseCtx, 0, inFlow)
	return outFlow, nil
}

func (s *OrderedMapStream[K, V]) launchActionOnFlow(ctx context.Context, action int, flow *flow[K, V]) *flow[K, V] {
	if action >= len(s.functions) {
		return flow
	}

	piper := s.functions[action]
	out := piper(ctx, flow)
	return s.launchActionOnFlow(ctx, action+1, out)
}
