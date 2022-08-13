package gs

import "golang.org/x/exp/constraints"

type (
	flow[K constraints.Ordered, V any] struct {
		ch   chan OrderedPair[K, V]
		stop chan struct{}
	}

	flowControl struct {
		concurrency uint8
	}

	FlowOption func(fc *flowControl)
)

func newFlow[K constraints.Ordered, V any](concurrency uint8) *flow[K, V] {
	return &flow[K, V]{
		ch:   make(chan OrderedPair[K, V]),
		stop: make(chan struct{}, concurrency),
	}
}

func Concurrency(n uint8) FlowOption {
	return func(fc *flowControl) {
		fc.concurrency = n
	}
}
