package set

type nothing struct{}

type Set[T comparable] interface {
	Insert(item T) (modified bool)
	Remove(item T) bool
	Clear()
	Has(item T) bool
	Items() []T
	InsertSet(sourceSet Set[T]) (modified bool)
}
