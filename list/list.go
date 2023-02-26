package list

import (
	"context"
	"errors"
	"sync"
)

type (
	flow[I, O any] struct {
		inCh        chan listItem[I]
		outCh       chan listItem[O]
		errCh       chan error
		closeCh     chan struct{}
		mapper      mapper[I, O]
		concurrency int
		tasks       sync.WaitGroup
	}

	listItem[V any] struct {
		idx   int
		value V
	}

	flowConfig[R any] struct {
		concurrency         int
		initialReducerValue R
	}

	reducerOption[R any] func(fc *flowConfig[R])

	mapper[I, O any]  func(int, I) (O, error)
	reducer[R, O any] func(R, int, O) (R, error)
)

func newFlow[I, O any](c int, m mapper[I, O]) *flow[I, O] {
	return &flow[I, O]{
		inCh:        make(chan listItem[I]),
		outCh:       make(chan listItem[O]),
		errCh:       make(chan error, c),
		closeCh:     make(chan struct{}),
		mapper:      m,
		concurrency: c,
	}
}

func (f *flow[I, O]) start(ctx context.Context) {
	for i := 0; i < f.concurrency; i++ {
		f.tasks.Add(1)

		go func() {
			defer f.tasks.Done()
			for {
				select {
				case item, ok := <-f.inCh:
					if !ok {
						return
					}
					result, err := f.mapper(item.idx, item.value)
					if err != nil {
						if errors.Is(err, ErrSkip) {
							continue
						}

						f.errCh <- err
						return
					}
					f.outCh <- listItem[O]{idx: item.idx, value: result}
				case <-f.closeCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (f *flow[I, O]) stop() {
	select {
	case <-f.closeCh:
		return
	default:
		close(f.closeCh)
	}

	f.tasks.Wait()

	close(f.inCh)
	close(f.outCh)
	close(f.errCh)
}

func feed[I, O any](ctx context.Context, in []I, f *flow[I, O]) {
	defer f.stop()
	for i, v := range in {
		select {
		case f.inCh <- listItem[I]{idx: i, value: v}:
			continue
		case <-f.closeCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func WithInitialValue[R any](r R) reducerOption[R] {
	return func(fc *flowConfig[R]) {
		fc.initialReducerValue = r
	}
}

func WithConcurrency[R any](c int) reducerOption[R] {
	return func(fc *flowConfig[R]) {
		fc.concurrency = c
	}
}

func MapReduce[I, O, R any](
	ctx context.Context,
	in []I,
	mapper mapper[I, O],
	reducer reducer[R, O],
	options ...reducerOption[R],
) (R, error) {
	cfg := flowConfig[R]{concurrency: 1, initialReducerValue: zero[R]()}
	for _, opt := range options {
		opt(&cfg)
	}

	f := newFlow(cfg.concurrency, mapper)
	f.start(ctx)

	go feed(ctx, in, f)

	acc, err := reduce(ctx, f, reducer, cfg.initialReducerValue)
	if err != nil {
		return cfg.initialReducerValue, err
	}

	return acc, nil
}

func reduce[R, O, I any](
	ctx context.Context,
	f *flow[I, O],
	r reducer[R, O],
	initialValue R,
) (R, error) {
	acc := initialValue
	for {
		select {
		case item, ok := <-f.outCh:
			if !ok {
				return acc, nil
			}
			var err error
			acc, err = r(acc, item.idx, item.value)
			if err != nil {
				f.stop()
				return acc, err
			}
		case <-ctx.Done():
			return acc, nil
		case err := <-f.errCh:
			f.stop()
			return acc, err
		}
	}
}

func zero[T any]() T {
	var z T
	return z
}
