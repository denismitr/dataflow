package keyvalue

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}
