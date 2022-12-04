package keyvalue

import "context"

type (
	// MustValueEffector us similar to value effector,
	// only it does not return an error
	ValueEffector[K comparable, V any] func(k K, v V) V

	// Reducer takes a carry from previous iteration that a key and a value
	// and returns a new version of carry
	Reducer[K comparable, V, R any] func(carry R, k K, v V) R

	// Predicate allows to filter key value pairs
	Predicate[K comparable, V any] func(k K, v V) bool

	PredicateContext[K comparable, V any] func(ctx context.Context, k K, v V) (bool, error)

	MapperContext[K comparable, V any] func(ctx context.Context, k K, v V) (V, error)

	IteratorContext[K comparable, V any] func(ctx context.Context, k K, v V) error
)

// Map is similar to map, only that it doest not return an error
func Map[K comparable, V any](
	in map[K]V,
	mvf ValueEffector[K, V],
) map[K]V {
	result := make(map[K]V, len(in))
	for k, v := range in {
		result[k] = mvf(k, v)
	}
	return result
}

// Filter a map by key and value
func Filter[K comparable, V any](
	in map[K]V,
	pred Predicate[K, V],
) map[K]V {
	result := make(map[K]V, len(in))
	for k, v := range in {
		if pred(k, v) {
			result[k] = v
		}

	}
	return result
}

// Reduce map to another generic value
func Reduce[K comparable, V, R any](
	in map[K]V,
	reducer Reducer[K, V, R],
) (R, error) {
	var r R
	for k, v := range in {
		r = reducer(r, k, v)
	}
	return r, nil
}
