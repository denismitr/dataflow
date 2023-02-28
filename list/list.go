package list

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type (
	flow[I, O any] struct {
		inCh    chan listItem[I]
		outCh   chan listItem[O]
		errCh   chan error
		closeCh chan struct{}
		mapper  mapper[I, O]
		fc      flowControl
		tasks   sync.WaitGroup
	}

	listItem[V any] struct {
		idx   int
		value V
	}

	flowControl struct {
		concurrency    int
		errorThreshold int
	}

	reducerOption func(fc *flowControl)

	mapper[I, O any]  func(int, I) (O, error)
	reducer[R, O any] func(R, int, O) (R, error)
)

func newFlow[I, O any](fc flowControl, m mapper[I, O]) *flow[I, O] {
	return &flow[I, O]{
		inCh:    make(chan listItem[I]),
		outCh:   make(chan listItem[O]),
		errCh:   make(chan error, fc.concurrency),
		closeCh: make(chan struct{}),
		mapper:  m,
		fc:      fc,
	}
}

func (f *flow[I, O]) start(ctx context.Context) {
	for i := 0; i < f.fc.concurrency; i++ {
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
					} else {
						f.outCh <- listItem[O]{idx: item.idx, value: result}
					}
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

	go func() {
		f.tasks.Wait()
		close(f.inCh)
		close(f.outCh)
		close(f.errCh)
	}()
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

func ErrorThreshold(et int) reducerOption {
	return func(fc *flowControl) {
		if et > 0 {
			fc.errorThreshold = et
		}
	}
}

func WithConcurrency(c int) reducerOption {
	return func(fc *flowControl) {
		fc.concurrency = c
	}
}

func MapReduce[I, O, R any](
	ctx context.Context,
	in []I,
	mapper mapper[I, O],
	reducer reducer[R, O],
	initialReducerValue R,
	options ...reducerOption,
) (R, error) {
	fc := flowControl{concurrency: 1, errorThreshold: 1}
	for _, opt := range options {
		opt(&fc)
	}

	f := newFlow(fc, mapper)
	f.start(ctx)

	go feed(ctx, in, f)

	acc, err := reduce(ctx, f, reducer, initialReducerValue)
	if err != nil {
		return acc, err
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
	var mpErr MapReduceError = nil

	for {
		select {
		case item, ok := <-f.outCh:
			if !ok {
				return acc, multiErrorOrNil(mpErr)
			}
			var err error
			acc, err = r(acc, item.idx, item.value)
			if err != nil {
				f.errCh <- fmt.Errorf("reduce error: %w", err)
			}
		case <-ctx.Done():
			f.stop()
			return acc, append(mpErr, ctx.Err())
		case err := <-f.errCh:
			mpErr = append(mpErr, err)
			if len(mpErr) >= f.fc.errorThreshold {
				f.stop()
				return acc, mpErr
			}
		}
	}
}

func multiErrorOrNil(mpErr MapReduceError) error {
	if len(mpErr) == 0 {
		return nil
	}

	return mpErr
}

func zero[T any]() T {
	var z T
	return z
}
