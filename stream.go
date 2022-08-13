package gs

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type (
	OrderedFilterFn[K constraints.Ordered, V any] func(key K, value V, order int) bool
	OrderedMapFn[K constraints.Ordered, V any]    func(key K, value V, order int) V

	orderedPiper[K constraints.Ordered, V any] func(ctx context.Context, flow *flow[K, V]) *flow[K, V]

	flow[K constraints.Ordered, V any] struct {
		ch   chan OrderedPair[K, V]
		stop chan struct{}
	}
)

func newFlow[K constraints.Ordered, V any](concurrency uint8) *flow[K, V] {
	return &flow[K, V]{
		ch:   make(chan OrderedPair[K, V]),
		stop: make(chan struct{}, concurrency),
	}
}

type OrderedMapStream[K constraints.Ordered, V any] struct {
	om          *OrderedMap[K, V]
	concurrency uint8

	mux     sync.Mutex
	actions []orderedPiper[K, V]
}

func (s *OrderedMapStream[K, V]) Filter(filter OrderedFilterFn[K, V]) *OrderedMapStream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](s.concurrency)

		var wg sync.WaitGroup
		wg.Add(int(s.concurrency))

		for i := 0; i < int(s.concurrency); i++ {
			go func() {
				defer wg.Done()

				for {
					select {
					case <-out.stop:
						flow.stop <- struct{}{}
						return
					case pair, ok := <-flow.ch:
						if ok {
							if v := filter(pair.Key, pair.Value, pair.Order); v {
								out.ch <- OrderedPair[K, V]{
									Key:   pair.Key,
									Value: pair.Value,
									Order: pair.Order,
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

	s.actions = append(s.actions, f)
	return s
}

func (s *OrderedMapStream[K, V]) Map(mapper OrderedMapFn[K, V]) *OrderedMapStream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](s.concurrency)

		var wg sync.WaitGroup
		wg.Add(int(s.concurrency))

		for i := 0; i < int(s.concurrency); i++ {
			go func() {
				defer wg.Done()

				for {
					select {
					case <-out.stop:
						flow.stop <- struct{}{}
						return
					case pair, ok := <-flow.ch:
						if ok {
							newValue := mapper(pair.Key, pair.Value, pair.Order)
							out.ch <- OrderedPair[K, V]{
								Key:   pair.Key,
								Value: newValue,
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

	s.actions = append(s.actions, f)
	return s
}

func (s *OrderedMapStream[K, V]) Take(n int) *OrderedMapStream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](s.concurrency)

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

	s.actions = append(s.actions, f)
	return s
}

func (s *OrderedMapStream[K, V]) Run(runCtx context.Context) (*OrderedMap[K, V], error) {
	if s.concurrency < 1 {
		return nil, errors.Wrapf(ErrInvalidConcurrency, "should be greater than 1, got %d", s.concurrency)
	}

	inFlow := newFlow[K, V](s.concurrency)

	go func() {
		ctx, cancel := context.WithCancel(runCtx)

		defer func() {
			cancel()
			close(inFlow.ch)
		}()

		for p := range s.om.pairs(ctx) {
			select {
			case inFlow.ch <- p:
			case <-inFlow.stop:
				return
			case <-runCtx.Done():
				return
			}
		}
	}()

	outFlow := s.launchActionOnFlow(runCtx, 0, inFlow)
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
		case <-runCtx.Done():
			return nil, runCtx.Err()
		}
	}

	sort.Sort(pairSlice)

	// todo: sort
	return fromOrderedPairs(pairSlice), nil
}

func join[K constraints.Ordered, V any](ctx context.Context, flows []*flow[K, V]) *flow[K, V] {
	out := newFlow[K, V](1)

	var wg sync.WaitGroup

	for i := range flows {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for {
				select {
				case p, ok := <-flows[idx].ch:
					if !ok {
						return
					}
					out.ch <- p
				case <-flows[idx].stop:
					return
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(out.ch)
	}()

	return out
}

func (s *OrderedMapStream[K, V]) launchActionOnFlow(ctx context.Context, action int, flow *flow[K, V]) *flow[K, V] {
	if action >= len(s.actions) {
		return flow
	}

	piper := s.actions[action]
	out := piper(ctx, flow)
	return s.launchActionOnFlow(ctx, action+1, out)
}
