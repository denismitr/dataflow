package set_test

import (
	"github.com/denismitr/dataflow/set"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOrderedSet_Remove(t *testing.T) {
	t.Run("remove existing item from the middle", func(t *testing.T) {
		s := set.NewOrderedSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		s.Remove("bar")

		items := s.Items()
		assert.Equal(t, []string{"foo", "baz", "123"}, items)
	})

	t.Run("remove existing item from the beginning", func(t *testing.T) {
		s := set.NewOrderedSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		s.Remove("foo")

		items := s.Items()
		assert.Equal(t, []string{"bar", "baz", "123"}, items)

		assert.False(t, s.Has("foo"))
		assert.True(t, s.Has("123"))
		assert.True(t, s.Has("bar"))
		assert.True(t, s.Has("baz"))
	})

	t.Run("remove existing item from the end", func(t *testing.T) {
		s := set.NewOrderedSet[string]()
		s.Insert("foo")
		s.Insert("bar")
		s.Insert("baz")
		s.Insert("123")

		assert.True(t, s.Remove("123"))

		items := s.Items()
		assert.False(t, s.Has("123"))
		assert.Equal(t, []string{"foo", "bar", "baz"}, items)
	})
}

func TestOrderedSet_InsertSet(t *testing.T) {
	t.Run("sets with single elements", func(t *testing.T) {
		s1 := set.NewOrderedSet[int]()
		s1.Insert(3)

		s2 := set.NewOrderedSet[int]()
		s2.Insert(9)

		assert.True(t, s1.InsertSet(s2))
		assert.Equal(t, 2, s1.Len())
		assert.Equal(t, 1, s2.Len())
		assert.True(t, s1.Has(3))
		assert.True(t, s1.Has(9))
		assert.False(t, s1.Has(1))

		items := s1.Items()
		assert.Equal(t, []int{3, 9}, items)
	})
}

func TestOrderedSet_InsertSlice(t *testing.T) {
	t.Run("set and slice with single elements", func(t *testing.T) {
		s1 := set.NewOrderedSet[int]()
		s1.Insert(3)

		assert.True(t, s1.InsertSlice([]int{9}))
		assert.Equal(t, 2, s1.Len())
		assert.True(t, s1.Has(3))
		assert.True(t, s1.Has(9))
		assert.False(t, s1.Has(1))

		items := s1.Items()
		assert.Equal(t, []int{3, 9}, items)
	})
}
