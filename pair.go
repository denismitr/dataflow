package gs

import "golang.org/x/exp/constraints"

type Pair[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}
