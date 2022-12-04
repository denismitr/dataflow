package utils

type Order uint8

const (
	DescOrder Order = iota
	AscOrder
)

func GetZero[T any]() T {
	var result T
	return result
}
