package superstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type (
	flowControl struct {
		concurrency    int
		errorThreshold int
	}

	reducerOption func(fc *flowControl)

	mapper[K comparable, I, O any] func(context.Context, Item[K, I]) (Item[K, O], error)
	reducer[K, R, O any]           func(context.Context, R, Item[K, O]) (R, error)
)

func doMap[K comparable, I, O any](
	ctx context.Context,
	fc *flowControl,
	inCh <-chan Item[K, I],
	mapper mapper[K, I, O],
) (<-chan Item[K, O], <-chan error) {
	resultCh := make(chan Item[K, O])
	errCh := make(chan error)
	var tasks sync.WaitGroup

	for i := 0; i < fc.concurrency; i++ {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			for {
				if err := ctx.Err(); err != nil {
					return
				}

				select {
				case item, ok := <-inCh:
					if !ok {
						return
					}
					result, err := mapper(ctx, item)
					if err != nil {
						if errors.Is(err, ErrSkip) {
							continue
						}

						errCh <- fmt.Errorf("map error: %w", err)
					} else {
						resultCh <- result
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		tasks.Wait()
		close(resultCh)
	}()

	return resultCh, errCh
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

func MapReduce[K comparable, I, O, R any](
	ctx context.Context,
	iterable Iterable[K, I],
	mapper mapper[K, I, O],
	reducer reducer[K, R, O],
	initialReducerValue R,
	options ...reducerOption,
) (R, error) {
	fc := &flowControl{concurrency: 1, errorThreshold: 1}
	for _, opt := range options {
		opt(fc)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inCh := iterable(ctx)
	outCh, mapErrCh := doMap(ctx, fc, inCh, mapper)
	acc, err := doReduce(ctx, outCh, mapErrCh, fc, reducer, initialReducerValue)
	if err != nil {
		return acc, err
	}

	return acc, nil
}

func doReduce[K comparable, R, O any](
	ctx context.Context,
	outCh <-chan Item[K, O],
	mapErrCh <-chan error,
	fc *flowControl,
	r reducer[K, R, O],
	initialValue R,
) (R, error) {
	acc := initialValue
	var mpErr MapReduceError = nil

	for {
		if len(mpErr) >= fc.errorThreshold {
			return acc, multiErrorOrNil(mpErr)
		}

		select {
		case item, ok := <-outCh:
			if !ok {
				return acc, multiErrorOrNil(nil)
			} else {
				var err error
				acc, err = r(ctx, acc, item)
				if err != nil {
					mpErr = append(mpErr, fmt.Errorf("reduce error: %w", err))
				}
			}
		case <-ctx.Done():
			ctxErr := ctx.Err()
			if errors.Is(ctxErr, context.Canceled) {
				return acc, multiErrorOrNil(mpErr)
			} else {
				return acc, append(mpErr, ctx.Err())
			}
		case err := <-mapErrCh:
			mpErr = append(mpErr, err)
		}
	}
}

func multiErrorOrNil(mpErr MapReduceError) error {
	if len(mpErr) == 0 {
		return nil
	}

	return mpErr
}
