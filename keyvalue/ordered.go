package keyvalue

import (
	"context"

	"github.com/denismitr/dll"
)

type (
	OrderedMap[K comparable, V any] struct {
		m    map[K]*dll.Element[Pair[K, V]]
		list *dll.DoublyLinkedList[Pair[K, V]]
	}

	OrderedFilterFn[K comparable, V any]       func(key K, value V, order int) bool
	OrderedForEachFn[K comparable, V any]      func(key K, value V, order int)
	OrderedForEachUntilFn[K comparable, V any] func(key K, value V, order int) bool
	OrderedMapFn[K comparable, V any]          func(key K, value V, order int) V
	LessPairFn[K comparable, V any]            func(a Pair[K, V], b Pair[K, V]) (less bool)
)

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		m:    make(map[K]*dll.Element[Pair[K, V]]),
		list: dll.New[Pair[K, V]](),
	}
}

// Set is idempotent
func (om *OrderedMap[K, V]) Set(key K, value V) {
	existingEl, found := om.m[key]
	if !found {
		p := Pair[K, V]{Key: key, Value: value}
		newEl := dll.NewElement(p)
		om.m[key] = newEl
		om.list.PushTail(newEl)
		return
	}

	existingEl.ReplaceValue(Pair[K, V]{Key: key, Value: value})
}

func (om *OrderedMap[K, V]) SetNX(key K, value V) (added bool) {
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

func (om *OrderedMap[K, V]) HasGet(key K) (V, bool) {
	el, found := om.m[key]
	if !found {
		return getZero[V](), false
	}

	return el.Value().Value, true
}

func (om *OrderedMap[K, V]) Get(key K) V {
	el, found := om.m[key]
	if !found {
		return getZero[V]()
	}

	return el.Value().Value
}

func (om *OrderedMap[K, V]) Has(key K) bool {
	_, found := om.m[key]
	return found
}

func (om *OrderedMap[K, V]) HasRemove(key K) (V, bool) {
	el, exists := om.m[key]
	if !exists {
		return getZero[V](), false
	}

	v := el.Value().Value
	delete(om.m, key)
	om.list.Remove(el)

	return v, true
}

func (om *OrderedMap[K, V]) Remove(key K) V {
	el, exists := om.m[key]
	if !exists {
		return getZero[V]()
	}

	v := el.Value().Value
	delete(om.m, key)
	om.list.Remove(el)

	return v
}

func (om *OrderedMap[K, V]) Pairs(ctx context.Context) <-chan Pair[K, V] {
	resultCh := make(chan Pair[K, V])

	go func() {
		defer close(resultCh)

		curr := om.list.Head()
		order := 0
		for curr != nil {
			if ctx.Err() != nil {
				return
			}

			pair := curr.Value()
			op := Pair[K, V]{
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

func (om *OrderedMap[K, V]) Stream(options ...FlowOption) *Stream[K, V] {
	return NewStream[K, V](om, options...)
}

func (om *OrderedMap[K, V]) Len() int {
	return len(om.m)
}

func (om *OrderedMap[K, V]) ForEach(f OrderedForEachFn[K, V]) {
	curr := om.list.Head()
	order := 0
	for curr != nil {
		f(curr.Value().Key, curr.Value().Value, order)
		curr = curr.Next()
		order++
	}
}

func (om *OrderedMap[K, V]) Map(f func(key K, value V, order int) V) *OrderedMap[K, V] {
	result := NewOrderedMap[K, V]()

	curr := om.list.Head()
	order := 0
	for curr != nil {
		p := curr.Value()
		mappedValue := f(p.Key, p.Value, order)
		result.Set(p.Key, mappedValue)
		curr = curr.Next()
	}

	return result
}

func (om *OrderedMap[K, V]) Filter(f OrderedFilterFn[K, V]) *OrderedMap[K, V] {
	result := NewOrderedMap[K, V]()

	curr := om.list.Head()
	order := 0
	for curr != nil {
		pair := curr.Value()
		preserve := f(pair.Key, pair.Value, order)
		if preserve {
			result.Set(pair.Key, pair.Value)
		}

		curr = curr.Next()
	}

	return result
}

func (om *OrderedMap[K, V]) ForEachUntil(ff OrderedForEachUntilFn[K, V]) *OrderedMap[K, V] {
	curr := om.list.Head()
	order := 0
	for curr != nil {
		pair := curr.Value()
		canGoOn := ff(pair.Key, pair.Value, order)
		if !canGoOn {
			break
		}

		curr = curr.Next()
	}

	return om
}

func (om *OrderedMap[K, V]) Clone() *OrderedMap[K, V] {
	result := NewOrderedMap[K, V]()

	curr := om.list.Head()
	for curr != nil {
		pair := curr.Value()
		result.Set(pair.Key, pair.Value)
		curr = curr.Next()
	}

	return result
}

// SortBy - sorts the collection and returns the sorted one
func (om *OrderedMap[K, V]) SortBy(lessFn LessPairFn[K, V]) *OrderedMap[K, V] {
	clone := om.Clone()
	clone.list.Sort(dll.CompareFn[Pair[K, V]](lessFn))
	return clone
}

// SortBy - sorts the collection in place
func (om *OrderedMap[K, V]) SortInPlaceBy(lessFn LessPairFn[K, V]) *OrderedMap[K, V] {
	om.list.Sort(dll.CompareFn[Pair[K, V]](lessFn))
	return om
}
