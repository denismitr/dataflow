package set

type Set[T comparable] struct {
	m     map[T]struct{}
	order []T
}

func New[T comparable]() *Set[T] {
	return &Set[T]{
		m: make(map[T]struct{}),
	}
}

func (s *Set[T]) Add(item T) *Set[T] {
	s.m[item] = struct{}{}
	s.order = append(s.order, item)
	return s
}

func (s *Set[T]) Clear() *Set[T] {
	s.m = make(map[T]struct{})
	s.order = nil
	return s
}

func (s *Set[T]) Items() []T {
	return s.order
}

func (s *Set[T]) Has(item T) bool {
	_, ok := s.m[item]
	return ok
}

func (s *Set[T]) Delete(item T) bool {
	if _, found := s.m[item]; found {
		delete(s.m, item)
		idx := -1
		for i := range s.order {
			if s.order[i] == item {
				idx = i
			}
		}
		if idx > -1 {
			s.order = append(s.order[:idx], s.order[idx+1:]...)
		}
	}

	return false
}
