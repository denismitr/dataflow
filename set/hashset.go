package set

type HashSet[T comparable] struct {
	m map[T]struct{}
}

var _ Set[int] = (*HashSet[int])(nil)

func New[T comparable]() *HashSet[T] {
	return &HashSet[T]{
		m: make(map[T]struct{}),
	}
}

func (s *HashSet[T]) Insert(item T) (modified bool) {
	if _, found := s.m[item]; !found {
		s.m[item] = struct{}{}
		modified = true
	}

	return modified
}

func (s *HashSet[T]) Clear() {
	s.m = nil
	s.m = make(map[T]struct{})
}

func (s *HashSet[T]) Items() []T {
	items := make([]T, 0, len(s.m))
	for item := range s.m {
		items = append(items, item)
	}
	return items
}

func (s *HashSet[T]) Has(item T) bool {
	_, ok := s.m[item]
	return ok
}

func (s *HashSet[T]) Remove(item T) bool {
	if _, found := s.m[item]; found {
		delete(s.m, item)
	}

	return false
}
