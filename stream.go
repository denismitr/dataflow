package gs

import (
	"context"
	"sync"

	"github.com/denismitr/dll"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type (
	FilterKeyValueFn[K constraints.Ordered, V any]    func(key K, value V) bool
	MapKeyValueFn[K constraints.Ordered, V any]       func(key K, value V) V
	ForEachKeyValueFn[K constraints.Ordered, V any]   func(key K, value V)
	ReduceKeyValueFn[K constraints.Ordered, V, R any] func(carry R, key K, value V) R
	FirstKeyValueFn[K constraints.Ordered, V any]     func(key K, value V) (bool, error)

	keyValuePipe[K constraints.Ordered, V any] func(ctx context.Context, flow *flow[K, V]) *flow[K, V]
)

type OrderedMapStream[K constraints.Ordered, V any] struct {
	om *OrderedMap[K, V]
	fc flowControl

	functions []keyValuePipe[K, V]
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
	filter FilterKeyValueFn[K, V],
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
							if v := filter(oPair.Key, oPair.Value); v {
								out.ch <- Pair[K, V]{
									Key:   oPair.Key,
									Value: oPair.Value,
								}
							}
						} else {
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
	mapper MapKeyValueFn[K, V],
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
					case pair, ok := <-flow.ch:
						if ok {
							newValue := mapper(pair.Key, pair.Value)
							out.ch <- Pair[K, V]{
								Key:   pair.Key,
								Value: newValue,
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

func (s *OrderedMapStream[K, V]) ForEach(
	effector ForEachKeyValueFn[K, V],
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
					case pair, ok := <-flow.ch:
						if ok {
							effector(pair.Key, pair.Value)
							out.ch <- Pair[K, V]{
								Key:   pair.Key,
								Value: pair.Value,
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

func (s *OrderedMapStream[K, V]) SortBy(lessFn LessPairFn[K, V]) *OrderedMapStream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](DefaultConcurrency)

		go func() {
			defer close(out.ch)

			tempList := dll.New[Pair[K, V]]()

			for {
				select {
				case pair, ok := <-flow.ch:
					if ok {
						tempList.PushTail(dll.NewElement(Pair[K, V]{Key: pair.Key, Value: pair.Value}))
					} else {
						tempList.Sort(dll.CompareFn[Pair[K, V]](lessFn))

						curr := tempList.Head()
						order := 0
						for curr != nil {
							out.ch <- Pair[K, V]{
								Key:   curr.Value().Key,
								Value: curr.Value().Value,
							}

							curr = curr.Next()
							order++
						}

						return
					}
				case <-out.stop:
					flow.stop <- struct{}{}
					return
				case <-ctx.Done():
					return
				}
			}
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
						out.ch <- Pair[K, V]{
							Key:   pair.Key,
							Value: pair.Value,
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
	matcher FirstKeyValueFn[K, V],
	options ...FlowOption,
) (Pair[K, V], error) {
	outFlow, err := s.run(baseCtx)
	if err != nil {
		return getZero[Pair[K, V]](), errors.Wrap(err, "failed to get the first value")
	}

	localFc := flowControl{
		concurrency: s.fc.concurrency,
	}

	for _, o := range options {
		o(&localFc)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	resultCh := make(chan Pair[K, V], localFc.concurrency)
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
				case pair, ok := <-outFlow.ch:
					if ok {
						match, err := matcher(pair.Key, pair.Value)
						if err != nil {
							errCh <- err
						}
						if match {
							resultCh <- pair
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
				return getZero[Pair[K, V]](), errors.Wrap(err, "no match found")
			}
		case <-ctx.Done():
			return getZero[Pair[K, V]](), errors.Wrap(ctx.Err(), "no match found")
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

	om := NewOrderedMap[K, V]()
	om.lockEnabled = false
	defer func() { om.lockEnabled = true }()

resultLoop:
	for {
		select {
		case result, ok := <-outFlow.ch:
			if ok {
				om.Put(result.Key, result.Value)
			} else {
				break resultLoop
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return om, nil
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

		for oPair := range s.om.Pairs(ctx) {
			select {
			case inFlow.ch <- Pair[K, V]{Key: oPair.Key, Value: oPair.Value}:
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
