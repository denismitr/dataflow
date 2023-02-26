package stream

import (
	"context"
	"sync"

	"github.com/denismitr/dataflow/utils"
)

const (
	DefaultConcurrency = 100
)

type (
	keyValuePipe[K comparable, V any] func(
		ctx context.Context,
		flow *flow[K, V],
	) *flow[K, V]

	StreamSource[K comparable, V any] interface {
		Pairs(context.Context) <-chan utils.Pair[K, V]
	}

	StreamDestination[K comparable, V any] interface {
		Set(k K, v V)
	}

	Stream[K comparable, V any] struct {
		source    StreamSource[K, V]
		fc        flowControl
		functions []keyValuePipe[K, V]
	}
)

func New[K comparable, V any](
	source StreamSource[K, V],
	options ...FlowOption,
) *Stream[K, V] {
	fc := flowControl{
		concurrency: DefaultConcurrency,
	}

	for _, o := range options {
		o(&fc)
	}

	return &Stream[K, V]{
		source: source,
		fc:     fc,
	}
}

func (s *Stream[K, V]) ForEach(
	iterator IteratorContext[K, V],
	options ...FlowOption,
) *Stream[K, V] {
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
							_ = iterator(ctx, pair.Key, pair.Value) // todo: handle error
							out.ch <- utils.Pair[K, V]{
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

// Filter the stream items
func (s *Stream[K, V]) Filter(
	predicate PredicateContext[K, V],
	options ...FlowOption,
) *Stream[K, V] {
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
							if v, err := predicate(ctx, oPair.Key, oPair.Value); err != nil {
								flow.stop <- struct{}{}
								// todo: flow.errors <- err
							} else if v {
								out.ch <- utils.Pair[K, V]{
									Key:   oPair.Key,
									Value: oPair.Value,
								}
							}
						} else {
							return
						}
					case <-ctx.Done():
						flow.stop <- struct{}{}
						// todo: flow.errors <- ctx.Err()
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

// Map the stream values
func (s *Stream[K, V]) Map(
	mapper MapperContext[K, V],
	options ...FlowOption,
) *Stream[K, V] {
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
							newValue, _ := mapper(ctx, pair.Key, pair.Value)
							out.ch <- utils.Pair[K, V]{
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

// Take n items from stream
func (s *Stream[K, V]) Take(n int) *Stream[K, V] {
	f := func(ctx context.Context, flow *flow[K, V]) *flow[K, V] {
		out := newFlow[K, V](DefaultConcurrency)

		go func() {
			defer close(out.ch)

			taken := 0
			for {
				select {
				case pair, ok := <-flow.ch:
					if ok {
						out.ch <- utils.Pair[K, V]{
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

// PipeInto a destination
func (s *Stream[K, V]) PipeInto(
	ctx context.Context,
	dst StreamDestination[K, V],
) error {
	outFlow, err := s.run(ctx)
	if err != nil {
		return err
	}

resultLoop:
	for {
		select {
		case result, ok := <-outFlow.ch:
			if ok {
				dst.Set(result.Key, result.Value)
			} else {
				break resultLoop
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *Stream[K, V]) run(baseCtx context.Context) (*flow[K, V], error) {
	// if s.fc.concurrency < 1 {
	// 	return nil, errors.Wrapf(ErrInvalidConcurrency, "should be greater than 1, got %d", s.fc.concurrency)
	// }

	inFlow := newFlow[K, V](s.fc.concurrency)

	go func() {
		ctx, cancel := context.WithCancel(baseCtx)

		defer func() {
			cancel()
			close(inFlow.ch)
		}()

		for pair := range s.source.Pairs(ctx) {
			select {
			case inFlow.ch <- pair:
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

func (s *Stream[K, V]) launchActionOnFlow(
	ctx context.Context,
	action int,
	flow *flow[K, V],
) *flow[K, V] {
	if action >= len(s.functions) {
		return flow
	}

	piper := s.functions[action]
	out := piper(ctx, flow)
	return s.launchActionOnFlow(ctx, action+1, out)
}
