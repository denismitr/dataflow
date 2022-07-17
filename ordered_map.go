package gs

import (
	"sync"

	"golang.org/x/exp/constraints"
)

type element[K constraints.Ordered, V any] struct {
	key   K
	value V
	next  *element[K, V]
	prev  *element[K, V]
}

type OrderedMap[K constraints.Ordered, V any] struct {
	m    map[K]*element[K, V]
	root *element[K, V]
	tail *element[K, V]
	mu   sync.RWMutex
}

func NewOrderedMap[K constraints.Ordered, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		m:    make(map[K]*element[K, V]),
		root: nil,
		tail: nil,
	}
}

// Put is idempotent and returns true if a new value was added
func (om *OrderedMap[K, V]) Put(key K, value V) bool {
	om.mu.Lock()
	defer om.mu.Unlock()

	existingEl, found := om.m[key]
	if !found {
		newEl := &element[K, V]{
			key:   key,
			value: value,
			prev:  om.tail,
		}
		om.m[key] = newEl
		om.tail = newEl
		return true
	}

	existingEl.value = value
	return false
}

func (om *OrderedMap[K, V]) Len() int {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return len(om.m)
}

func (om *OrderedMap[K, V]) Get(key K) (V, bool) {
	om.mu.RLock()
	defer om.mu.RUnlock()
	el, found := om.m[key]
	if !found {
		return getZero[V](), false
	}

	return el.value, true
}

func getZero[T any]() T {
	var result T
	return result
}
