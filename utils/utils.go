package utils

type Order uint8

const (
	DescOrder Order = iota
	AscOrder
)

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

func GetZero[T any]() T {
	var result T
	return result
}
