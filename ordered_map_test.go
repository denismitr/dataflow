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

func TestOrderedMap_Remove(t *testing.T) {
	t.Run("remove all existing keys starting from the middle", func(t *testing.T) {
		om := gs.NewOrderedMap[string, string]()
		om.Put("foo", "1")
		om.Put("bar", "2")
		om.Put("baz", "5")
		om.Put("123abc", "444")
		om.Put("abc", "123")
		om.Put("abc123", "321")
		om.Put("abc-000", "000abc")

		assert.Equal(t, 7, om.Len())

		assert.True(t, om.Remove("baz"))
		assert.True(t, om.Remove("123abc"))
		assert.True(t, om.Remove("abc"))

		assert.Equal(t, 4, om.Len())

		assert.True(t, om.Remove("abc123"))
		assert.True(t, om.Remove("bar"))
		assert.True(t, om.Remove("foo"))
		assert.True(t, om.Remove("abc-000"))

		assert.Equal(t, 0, om.Len())
	})
}

func TestOrderedMap_ForEach(t *testing.T) {
	t.Run("iterate over an empty map", func(t *testing.T) {
		iterations := 0
		om := gs.NewOrderedMap[string, string]()
		om.ForEach(func(k string, v string, order int) {
			iterations++
		})
		assert.Equal(t, 0, iterations)
	})

	t.Run("iterate over values in map", func(t *testing.T) {
		iterations := 0
		var keyOrder []string

		om := gs.NewOrderedMap[string, string]()
		om.Put("foo", "1")
		om.Put("bar", "2")
		om.Put("baz", "5")
		om.Put("123abc", "444")
		om.Put("abc", "123")
		om.Put("abc", "124")
		om.Put("abc123", "321")
		om.Put("abc-000", "000abc")

		om.ForEach(func(k string, v string, order int) {
			iterations++
			keyOrder = append(keyOrder, k)
		})

		assert.Equal(t, 7, iterations)
		assert.Equal(t, 7, len(keyOrder))
	})
}
