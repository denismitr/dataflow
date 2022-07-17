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

		if om.root == nil {
			om.root = newEl
			om.tail = newEl
		} else {
			prev := om.tail
			prev.next = newEl
			newEl.prev = prev
			om.tail = newEl
		}

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

func (om *OrderedMap[K, V]) Remove(key K) bool {
	om.mu.Lock()
	defer om.mu.Unlock()

	el, found := om.m[key]
	if !found {
		return false
	}

	delete(om.m, key)

	// is root
	if el.prev == nil {
		om.root = nil
		om.tail = nil
		return true
	}

	// is tail
	if el.next == nil {
		om.tail = el.prev
		el.prev.next = nil
		return true
	}

	el.prev.next = el.next
	el.next.prev = el.prev
	el.next = nil
	el.prev = nil
	return true
}

func (om *OrderedMap[K, V]) ForEach(f func(key K, value V, order int)) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	curr := om.root
	order := 0
	for curr != nil {
		f(curr.key, curr.value, order)
		curr = curr.next
	}
}

func (om *OrderedMap[K, V]) Map(f func(key K, value V, order int) V) *OrderedMap[K, V] {
	om.mu.RLock()
	defer om.mu.RUnlock()

	result := NewOrderedMap[K, V]()

	curr := om.root
	order := 0
	for curr != nil {
		mappedValue := f(curr.key, curr.value, order)
		result.Put(curr.key, mappedValue)
		curr = curr.next
	}

	return result
}

func getZero[T any]() T {
	var result T
	return result
}
