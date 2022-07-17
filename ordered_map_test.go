package gs_test

import (
	"testing"

	"github.com/denismitr/gs"
	"github.com/stretchr/testify/assert"
)

func TestOrderedMap_Len(t *testing.T) {
	t.Run("after put", func(t *testing.T) {
		om := gs.NewOrderedMap[string, int]()
		om.Put("foo", 1)
		om.Put("bar", 2)

		assert.Equal(t, 2, om.Len())

		om.Put("foo", 3)
		om.Put("baz", 123)

		assert.Equal(t, 3, om.Len())
	})
}

func TestOrderedMap_Get(t *testing.T) {
	t.Run("get existing and non existing value", func(t *testing.T) {
		om := gs.NewOrderedMap[string, int]()
		om.Put("foo", 1)
		om.Put("bar", 2)

		fooV, ok := om.Get("foo")
		assert.True(t, ok)
		assert.Equal(t, 1, fooV)

		barV, ok := om.Get("bar")
		assert.True(t, ok)
		assert.Equal(t, 2, barV)

		nilV, ok := om.Get("non-existent")
		assert.False(t, ok)
		assert.Equal(t, 0, nilV)
	})
}
