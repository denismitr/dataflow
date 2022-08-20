package gs

import (
	"context"
	"sync"

	"github.com/denismitr/dll"
	"golang.org/x/exp/constraints"
)

type LessPairFn[K constraints.Ordered, V any] func(a Pair[K, V], b Pair[K, V]) (less bool)

type OrderedMap[K constraints.Ordered, V any] struct {
	m           map[K]*dll.Element[Pair[K, V]]
	list        *dll.DoublyLinkedList[Pair[K, V]]
	lockEnabled bool
	mu          sync.RWMutex
}

func NewOrderedMap[K constraints.Ordered, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		m:           make(map[K]*dll.Element[Pair[K, V]]),
		list:        dll.New[Pair[K, V]](),
		lockEnabled: true,
	}
}

func fromOrderedPairs[K constraints.Ordered, V any](pairs []OrderedPair[K, V]) *OrderedMap[K, V] {
	om := &OrderedMap[K, V]{
		m:           make(map[K]*dll.Element[Pair[K, V]]),
		list:        dll.New[Pair[K, V]](),
		lockEnabled: false,
	}

	for i := range pairs {
		om.Put(pairs[i].Key, pairs[i].Value)
	}

	om.lockEnabled = true

	return om
}

const DefaultConcurrency = 1

func (om *OrderedMap[K, V]) Stream(options ...FlowOption) *OrderedMapStream[K, V] {
	fc := flowControl{
		concurrency: DefaultConcurrency,
	}

	for _, o := range options {
		o(&fc)
	}

	return &OrderedMapStream[K, V]{
		om: om,
		fc: fc,
	}
}

// Put is idempotent and returns true if a new value was added
func (om *OrderedMap[K, V]) Put(key K, value V) (added bool) {
	if om.lockEnabled {
		om.mu.Lock()
		defer om.mu.Unlock()
	}

	existingEl, found := om.m[key]
	if !found {
		p := Pair[K, V]{Key: key, Value: value}
		newEl := dll.NewElement(p)
		om.m[key] = newEl
		om.list.PushTail(newEl)
		return true
	}

	existingEl.ReplaceValue(Pair[K, V]{Key: key, Value: value})
	return false
}

func (om *OrderedMap[K, V]) PutNX(key K, value V) (added bool) {
	if om.lockEnabled {
		om.mu.Lock()
		defer om.mu.Unlock()
	}

	_, found := om.m[key]
	if found {
		return false
	}

	p := Pair[K, V]{Key: key, Value: value}
	newEl := dll.NewElement(p)
	om.m[key] = newEl
	om.list.PushTail(newEl)
	return true
}

func (om *OrderedMap[K, V]) Len() int {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	return len(om.m)
}

func (om *OrderedMap[K, V]) Get(key K) (V, bool) {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	el, found := om.m[key]
	if !found {
		return getZero[V](), false
	}

	return el.Value().Value, true
}

func (om *OrderedMap[K, V]) Remove(key K) (found bool) {
	if om.lockEnabled {
		om.mu.Lock()
		defer om.mu.Unlock()
	}

	el, exists := om.m[key]
	if !exists {
		return false
	}

	delete(om.m, key)
	om.list.Remove(el)

	return true
}

func (om *OrderedMap[K, V]) ForEach(f func(key K, value V, order int)) {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	curr := om.list.Head()
	order := 0
	for curr != nil {
		f(curr.Value().Key, curr.Value().Value, order)
		curr = curr.Next()
		order++
	}
}

func (om *OrderedMap[K, V]) Map(f func(key K, value V, order int) V) *OrderedMap[K, V] {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	result := NewOrderedMap[K, V]()
	result.lockEnabled = false

	curr := om.list.Head()
	order := 0
	for curr != nil {
		p := curr.Value()
		mappedValue := f(p.Key, p.Value, order)
		result.Put(p.Key, mappedValue)
		curr = curr.Next()
	}

	result.lockEnabled = true
	return result
}

func (om *OrderedMap[K, V]) Reduce(f func(key K, value V, order int) bool) *OrderedMap[K, V] {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	result := NewOrderedMap[K, V]()
	result.lockEnabled = false

	curr := om.list.Head()
	order := 0
	for curr != nil {
		p := curr.Value()
		exclude := f(p.Key, p.Value, order)
		if !exclude {
			result.Put(p.Key, p.Value)
		}

		curr = curr.Next()
	}

	result.lockEnabled = true
	return result
}

func (om *OrderedMap[K, V]) Filter(f func(key K, value V, order int) bool) *OrderedMap[K, V] {
	if om.lockEnabled {
		om.mu.RLock()
		defer om.mu.RUnlock()
	}

	result := NewOrderedMap[K, V]()
	result.lockEnabled = false

	curr := om.list.Head()
	order := 0
	for curr != nil {
		pair := curr.Value()
		preserve := f(pair.Key, pair.Value, order)
		if preserve {
			result.Put(pair.Key, pair.Value)
		}

		curr = curr.Next()
	}

	result.lockEnabled = true
	return result
}

func (om *OrderedMap[K, V]) SortBy(lessFn LessPairFn[K, V]) {
	om.list.Sort(dll.CompareFn[Pair[K, V]](lessFn))
}

func (om *OrderedMap[K, V]) Pairs(ctx context.Context) <-chan OrderedPair[K, V] {
	resultCh := make(chan OrderedPair[K, V])

	go func() {
		om.mu.RLock()
		defer om.mu.RUnlock()
		defer close(resultCh)

		curr := om.list.Head()
		order := 0
		for curr != nil {
			if ctx.Err() != nil {
				return
			}

			pair := curr.Value()
			op := OrderedPair[K, V]{
				Order: order,
				Key:   pair.Key,
				Value: pair.Value,
			}

			order++

			resultCh <- op

			curr = curr.Next()
		}
	}()

	return resultCh
}
