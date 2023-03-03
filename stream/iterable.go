package superstream

import (
	"context"
)

type Item[K any, V any] struct {
	Key   K
	Value V
}

type Iterable[K, V any] func(ctx context.Context) <-chan Item[K, V]

func Slice[V any](items []V) Iterable[int, V] {
	return func(ctx context.Context) <-chan Item[int, V] {
		resultCh := make(chan Item[int, V])
		go func() {
			defer close(resultCh)
			for i := 0; i < len(items); i++ {
				select {
				case <-ctx.Done():
					return
				case resultCh <- Item[int, V]{Key: i, Value: items[i]}:
				}
			}
		}()
		return resultCh
	}
}

func Map[K comparable, V any](m map[K]V) Iterable[K, V] {
	return func(ctx context.Context) <-chan Item[K, V] {
		resultCh := make(chan Item[K, V])
		go func() {
			defer close(resultCh)
			for k, v := range m {
				select {
				case <-ctx.Done():
					return
				case resultCh <- Item[K, V]{Key: k, Value: v}:
				}
			}
		}()
		return resultCh
	}
}

func Zero[T any]() T {
	var zero T
	return zero
}
