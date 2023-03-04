package set_test

import (
	"github.com/denismitr/dataflow/set"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestHashSet_Remove(t *testing.T) {
	t.Run("remove existing item from the middle", func(t *testing.T) {
		s := set.NewHashSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		s.Remove("bar")

		items := s.Items()
		sort.Strings(items)

		assert.Equal(t, []string{"123", "baz", "foo"}, items)
	})

	t.Run("remove existing item from the beginning", func(t *testing.T) {
		s := set.NewHashSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		s.Remove("foo")

		items := s.Items()
		sort.Strings(items)
		assert.Equal(t, []string{"123", "bar", "baz"}, items)

		assert.False(t, s.Has("foo"))
		assert.True(t, s.Has("123"))
		assert.True(t, s.Has("bar"))
		assert.True(t, s.Has("baz"))
	})

	t.Run("remove existing item from the end", func(t *testing.T) {
		s := set.NewHashSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		s.Remove("123")

		items := s.Items()
		sort.Strings(items)
		assert.Equal(t, []string{"bar", "baz", "foo"}, items)
		assert.False(t, s.Has("123"))
	})
}

func TestHashSet_InsertSet(t *testing.T) {
	t.Run("sets with single elements", func(t *testing.T) {
		s1 := set.NewHashSet[int]()
		s1.Insert(3)

		s2 := set.NewHashSet[int]()
		s2.Insert(9)

		assert.True(t, s1.InsertSet(s2))
		assert.Equal(t, 2, s1.Len())
		assert.Equal(t, 1, s2.Len())
		assert.True(t, s1.Has(3))
		assert.True(t, s1.Has(9))
		assert.False(t, s1.Has(1))

		items := s1.Items()
		sort.Ints(items)
		assert.Equal(t, []int{3, 9}, items)
	})
}

func TestHashSet_InsertSlice(t *testing.T) {
	t.Run("set and slice with single elements", func(t *testing.T) {
		s1 := set.NewHashSet[int]()
		s1.Insert(3)

		assert.True(t, s1.InsertSlice([]int{9}))
		assert.Equal(t, 2, s1.Len())
		assert.True(t, s1.Has(3))
		assert.True(t, s1.Has(9))
		assert.False(t, s1.Has(1))

		items := s1.Items()
		sort.Ints(items)
		assert.Equal(t, []int{3, 9}, items)
	})

	t.Run("set and slice with multiple elements", func(t *testing.T) {
		s1 := set.NewHashSet[int]()
		s1.Insert(3)
		s1.Insert(20)
		s1.Insert(333)

		assert.True(t, s1.InsertSlice([]int{9, 8, -1, 99, 55}))
		assert.Equal(t, 8, s1.Len())
		assert.True(t, s1.Has(3))
		assert.True(t, s1.Has(-1))
		assert.True(t, s1.Has(99))
		assert.True(t, s1.Has(20))
		assert.False(t, s1.Has(21))

		items := s1.Items()
		sort.Ints(items)
		assert.Equal(t, []int{-1, 3, 8, 9, 20, 55, 99, 333}, items)
	})
}
