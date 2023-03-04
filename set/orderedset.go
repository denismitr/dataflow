package set

import (
	"github.com/denismitr/dll"
)

type OrderedSet[T comparable] struct {
	m    map[T]*dll.Element[T]
	list *dll.DoublyLinkedList[T]
}

func NewOrderedSet[T comparable]() *OrderedSet[T] {
	return &OrderedSet[T]{
		m:    make(map[T]*dll.Element[T]),
		list: dll.New[T](),
	}
}

var _ Set[int] = (*HashSet[int])(nil)

func (s *OrderedSet[T]) Insert(item T) (modified bool) {
	if _, found := s.m[item]; !found {
		newEl := dll.NewElement(item)
		s.m[item] = newEl
		s.list.PushTail(newEl)
		modified = true
	}

	return modified
}

func (s *OrderedSet[T]) Clear() {
	s.m = nil
	s.m = make(map[T]*dll.Element[T])
	s.list = nil
	s.list = dll.New[T]()
}

func (s *OrderedSet[T]) Remove(item T) bool {
	if el, found := s.m[item]; found {
		delete(s.m, el.Value())
		s.list.Remove(el)
		return true
	}

	return false
}

func (s *OrderedSet[T]) Items() []T {
	items := make([]T, 0, len(s.m))
	curr := s.list.Head()
	for curr != nil {
		item := curr.Value()
		items = append(items, item)
		curr = curr.Next()
	}
	return items
}

func (s *OrderedSet[T]) Has(item T) bool {
	_, ok := s.m[item]
	return ok
}
