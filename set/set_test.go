package set_test

import (
	"github.com/denismitr/dataflow/set"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet_Delete(t *testing.T) {
	t.Run("delete existing item from the middle", func(t *testing.T) {
		s := set.New[string]()
		s.Add("foo").Add("bar").Add("baz").Add("123")

		s.Delete("bar")

		assert.Equal(t, []string{"foo", "baz", "123"}, s.Items())
	})

	t.Run("delete existing item from the beginning", func(t *testing.T) {
		s := set.New[string]()
		s.Add("foo").Add("bar").Add("baz").Add("123")

		s.Delete("foo")

		assert.Equal(t, []string{"bar", "baz", "123"}, s.Items())
		assert.False(t, s.Has("foo"))
		assert.True(t, s.Has("123"))
		assert.True(t, s.Has("bar"))
		assert.True(t, s.Has("baz"))
	})

	t.Run("delete existing item from the end", func(t *testing.T) {
		s := set.New[string]()
		s.Add("foo").Add("bar").Add("baz").Add("123")

		s.Delete("123")

		assert.Equal(t, []string{"foo", "bar", "baz"}, s.Items())
		assert.False(t, s.Has("123"))
	})
}
