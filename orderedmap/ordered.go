package orderedmap

import (
	"context"
	"github.com/denismitr/dataflow/utils"

	"github.com/denismitr/dll"
)

type (
	OrderedMap[K comparable, V any] struct {
		m    map[K]*dll.Element[utils.Pair[K, V]]
		list *dll.DoublyLinkedList[utils.Pair[K, V]]
	}

	FilterFn[K comparable, V any]       func(key K, value V, order int) bool
	ForEachFn[K comparable, V any]      func(key K, value V, order int)
	ForEachUntilFn[K comparable, V any] func(key K, value V, order int) bool
	TransformerFn[K comparable, V any]  func(key K, value V, order int) V
	LessPairFn[K comparable, V any]     func(a utils.Pair[K, V], b utils.Pair[K, V]) (less bool)
)

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		m:    make(map[K]*dll.Element[utils.Pair[K, V]]),
		list: dll.New[utils.Pair[K, V]](),
	}
}

// Set is idempotent
func (om *OrderedMap[K, V]) Set(key K, value V) {
	existingEl, found := om.m[key]
	if !found {
		p := utils.Pair[K, V]{Key: key, Value: value}
		newEl := dll.NewElement(p)
		om.m[key] = newEl
		om.list.PushTail(newEl)
		return
	}

	existingEl.ReplaceValue(utils.Pair[K, V]{Key: key, Value: value})
}

func (om *OrderedMap[K, V]) SetNX(key K, value V) (added bool) {
	_, found := om.m[key]
	if found {
		return false
	}

	p := utils.Pair[K, V]{Key: key, Value: value}
	newEl := dll.NewElement(p)
	om.m[key] = newEl
	om.list.PushTail(newEl)
	return true
}

func (om *OrderedMap[K, V]) HasGet(key K) (V, bool) {
	el, found := om.m[key]
	if !found {
		return utils.GetZero[V](), false
	}

	return el.Value().Value, true
}

func (om *OrderedMap[K, V]) Get(key K) V {
	el, found := om.m[key]
	if !found {
		return utils.GetZero[V]()
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
		return utils.GetZero[V](), false
	}

	v := el.Value().Value
	delete(om.m, key)
	om.list.Remove(el)

	return v, true
}

func (om *OrderedMap[K, V]) Remove(key K) V {
	el, exists := om.m[key]
	if !exists {
		return utils.GetZero[V]()
	}

	v := el.Value().Value
	delete(om.m, key)
	om.list.Remove(el)

	return v
}

func (om *OrderedMap[K, V]) Pairs(ctx context.Context) <-chan utils.Pair[K, V] {
	resultCh := make(chan utils.Pair[K, V])

	go func() {
		defer close(resultCh)

		curr := om.list.Head()
		order := 0
		for curr != nil {
			if ctx.Err() != nil {
				return
			}

			pair := curr.Value()
			op := utils.Pair[K, V]{
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

func (om *OrderedMap[K, V]) Len() int {
	return len(om.m)
}

func (om *OrderedMap[K, V]) ForEach(f ForEachFn[K, V]) {
	curr := om.list.Head()
	order := 0
	for curr != nil {
		f(curr.Value().Key, curr.Value().Value, order)
		curr = curr.Next()
		order++
	}
}

func (om *OrderedMap[K, V]) Transform(f TransformerFn[K, V]) *OrderedMap[K, V] {
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

func (om *OrderedMap[K, V]) Filter(f FilterFn[K, V]) *OrderedMap[K, V] {
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

func (om *OrderedMap[K, V]) ForEachUntil(ff ForEachUntilFn[K, V]) *OrderedMap[K, V] {
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
	clone.list.Sort(dll.LessFn[utils.Pair[K, V]](lessFn))
	return clone
}

// SortInPlaceBy - sorts the collection in place
func (om *OrderedMap[K, V]) SortInPlaceBy(lessFn LessPairFn[K, V]) *OrderedMap[K, V] {
	om.list.Sort(dll.LessFn[utils.Pair[K, V]](lessFn))
	return om
}
