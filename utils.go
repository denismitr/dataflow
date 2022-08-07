package gs

type Order uint8

const (
	DescOrder Order = iota
	AscOrder
)

func getZero[T any]() T {
	var result T
	return result
}
