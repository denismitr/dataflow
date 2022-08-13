package gs

import "golang.org/x/exp/constraints"

type Pair[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

type OrderedPair[K constraints.Ordered, V any] struct {
	Order int
	Key   K
	Value V
}

type OrderedPairs[K constraints.Ordered, V any] []OrderedPair[K, V]

func (op OrderedPairs[K, V]) Len() int {
	return len(op)
}

func (op OrderedPairs[K, V]) Swap(i, j int) {
	op[i], op[j] = op[j], op[i]
}

func (op OrderedPairs[K, V]) Less(i, j int) bool {
	return op[i].Order < op[j].Order
}
