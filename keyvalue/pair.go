package keyvalue

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

// type OrderedPair[K comparable, V any] struct {
// 	Order int
// 	Key   K
// 	Value V
// }

// type OrderedPairs[K comparable, V any] []OrderedPair[K, V]

// func (op OrderedPairs[K, V]) Len() int {
// 	return len(op)
// }

// func (op OrderedPairs[K, V]) Swap(i, j int) {
// 	op[i], op[j] = op[j], op[i]
// }

// func (op OrderedPairs[K, V]) Less(i, j int) bool {
// 	return op[i].Order < op[j].Order
// }
