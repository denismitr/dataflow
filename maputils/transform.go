package maputils

import (
	"context"
	"sync"
)

type (
	ValueTransformer[K comparable, V any]      func(K, V) V
	KeyValueTransformer[K comparable, V any]   func(K, V) (K, V)
	ValueTransformerAsync[K comparable, V any] func(context.Context, K, V) (V, error)
)

// Transform transforms a map by applying a value transformer callback to each map key value pair.
func Transform[K comparable, V any](m map[K]V, vt ValueTransformer[K, V]) map[K]V {
	result := make(map[K]V, len(m))
	for k, v := range m {
		transformed := vt(k, v)
		result[k] = transformed
	}
	return result
}

func TransformValues[K comparable, V any](m map[K]V, vt ValueTransformer[K, V]) map[K]V {
	return Transform(m, vt)
}

// TransformWithKeys transforms a map by applying a key value transformer callback
// to each map key value pair.
func TransformWithKeys[K comparable, V any](m map[K]V, kvt KeyValueTransformer[K, V]) map[K]V {
	result := make(map[K]V, len(m))
	for k, v := range m {
		tk, tv := kvt(k, v)
		result[tk] = tv
	}
	return result
}

func TransformAsync[K comparable, V any](
	baseCtx context.Context,
	m map[K]V,
	vt ValueTransformerAsync[K, V],
	concurrency uint32,
) (map[K]V, error) {
	return TransformValuesAsync(baseCtx, m, vt, concurrency)
}

func TransformValuesAsync[K comparable, V any](
	baseCtx context.Context,
	m map[K]V,
	vt ValueTransformerAsync[K, V],
	concurrency uint32,
) (map[K]V, error) {
	result := make(map[K]V, len(m))
	c := int(concurrency)
	if c < len(m) {
		c = len(m)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	var mux sync.Mutex
	errCh := make(chan error)
	doneCh := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		sem := make(chan struct{}, c)
		for k, v := range m {
			sem <- struct{}{}
			wg.Add(1)
			go func(k K, v V) {
				defer func() {
					<-sem
					wg.Done()
				}()

				transformed, err := vt(ctx, k, v)
				if err != nil {
					errCh <- err
				} else {
					mux.Lock()
					result[k] = transformed
					mux.Unlock()
				}
			}(k, v)
		}
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return result, nil
	case err := <-errCh:
		cancel()
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
