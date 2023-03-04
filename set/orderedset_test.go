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
