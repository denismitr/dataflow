package set_test

import (
	"github.com/denismitr/dataflow/set"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestHashSet_Remove(t *testing.T) {
	t.Run("remove existing item from the middle", func(t *testing.T) {
		s := set.New[string]()
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
		s := set.New[string]()
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
		s := set.New[string]()
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
