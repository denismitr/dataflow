package keyvalue

type (
	flowControl struct {
		concurrency uint32
	}

	flow[K comparable, V any] struct {
		ch   chan Pair[K, V]
		stop chan struct{}
	}

	FlowOption func(fc *flowControl)
)

func newFlow[K comparable, V any](concurrency uint32) *flow[K, V] {
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
