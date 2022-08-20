package gs

import "golang.org/x/exp/constraints"

type (
	flow[K constraints.Ordered, V any] struct {
		ch   chan Pair[K, V]
		stop chan struct{}
	}

	flowControl struct {
		concurrency uint32
	}

	FlowOption func(fc *flowControl)
)

func newFlow[K constraints.Ordered, V any](concurrency uint32) *flow[K, V] {
	return &flow[K, V]{
		ch:   make(chan Pair[K, V]),
		stop: make(chan struct{}, concurrency),
	}
}

func Concurrency(n uint32) FlowOption {
	return func(fc *flowControl) {
		fc.concurrency = n
	}
}
