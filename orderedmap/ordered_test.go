package orderedmap_test

import (
	"fmt"
	"testing"

	"github.com/denismitr/dataflow/orderedmap"
	"github.com/denismitr/dataflow/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderedMap_Len(t *testing.T) {
	t.Run("after put", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[string, int]()
		om.Set("foo", 1)
		om.Set("bar", 2)

		assert.Equal(t, 2, om.Len())

		om.Set("foo", 3)
		om.Set("baz", 123)

		assert.Equal(t, 3, om.Len())
	})
}

func TestOrderedMap_Get(t *testing.T) {
	t.Run("get existing and non existing value", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[string, int]()
		om.Set("foo", 1)
		om.Set("bar", 2)

		fooV, ok := om.HasGet("foo")
		assert.True(t, ok)
		assert.Equal(t, 1, fooV)

		barV, ok := om.HasGet("bar")
		assert.True(t, ok)
		assert.Equal(t, 2, barV)

		nilV, ok := om.HasGet("non-existent")
		assert.False(t, ok)
		assert.Equal(t, 0, nilV)
	})
}

func TestOrderedMap_Set(t *testing.T) {
	t.Run("it will override a value", func(t *testing.T) {
		const N = 1_000

		om := orderedmap.NewOrderedMap[string, int]()
		for i := 0; i < N; i++ {
			om.Set(fmt.Sprintf("key_%d", i), i)
		}

		for i := 0; i < N; i++ {
			om.Set(fmt.Sprintf("key_%d", i), i+N)
		}

		om.ForEach(func(key string, value int, order int) {
			assert.GreaterOrEqual(t, value, N-1, "value should be greater than N - 1")
			assert.LessOrEqual(t, order, N-1, "order should never be greater than N - 1")
			assert.Equal(t, fmt.Sprintf("key_%d", order), key, "key should follow pattern key_%d where %d is order")
			assert.Equal(t, order+N, value, "value should equal to order + N")
		})
	})
}

func TestOrderedMap_SetNX(t *testing.T) {
	t.Run("it will never override a value", func(t *testing.T) {
		const N = 1_000

		om := orderedmap.NewOrderedMap[string, int]()

		for i := 0; i < N; i++ {
			om.SetNX(fmt.Sprintf("key_%d", i), i)
		}

		for i := 0; i < N; i++ {
			om.SetNX(fmt.Sprintf("key_%d", i), i+N)
		}

		om.ForEach(func(key string, value int, order int) {
			assert.LessOrEqual(t, value, N-1, "value should never be greater than N - 1")
			assert.LessOrEqual(t, order, N-1, "order should never be greater than N - 1")
			assert.Equal(t, fmt.Sprintf("key_%d", value), key, "key should follow pattern key_%d where %d is value")
			assert.Equal(t, fmt.Sprintf("key_%d", order), key, "key should follow pattern key_%d where %d is order")
		})
	})
}

func TestOrderedMap_Remove(t *testing.T) {
	t.Run("remove all existing keys starting from the middle", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[string, string]()
		om.Set("foo", "1")
		om.Set("bar", "2")
		om.Set("baz", "5")
		om.Set("123abc", "444")
		om.Set("abc", "123")
		om.Set("abc123", "321")
		om.Set("abc-000", "000abc")

		assert.Equal(t, 7, om.Len())

		om.Remove("baz")
		om.Remove("123abc")
		om.Remove("abc")

		assert.Equal(t, 4, om.Len())

		om.Remove("abc123")
		om.Remove("bar")
		om.Remove("foo")
		om.Remove("abc-000")

		assert.Equal(t, 0, om.Len())
	})
}

func TestOrderedMap_HasRemove(t *testing.T) {
	t.Run("remove all existing keys starting from the middle", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[string, string]()
		om.Set("foo", "1")
		om.Set("bar", "2")
		om.Set("baz", "5")
		om.Set("123abc", "444")
		om.Set("abc", "123")
		om.Set("abc123", "321")
		om.Set("abc-000", "000abc")

		assert.Equal(t, 7, om.Len())

		{
			v, ok := om.HasRemove("baz")
			assert.True(t, ok)
			assert.Equal(t, "5", v)
		}

		{
			v, ok := om.HasRemove("123abc")
			assert.True(t, ok)
			assert.Equal(t, "444", v)
		}

		{
			v, ok := om.HasRemove("abc")
			assert.True(t, ok)
			assert.Equal(t, "123", v)
		}

		assert.Equal(t, 4, om.Len())

		{
			v, ok := om.HasRemove("non-existent")
			assert.False(t, ok)
			assert.Equal(t, "", v)
		}

		assert.Equal(t, 4, om.Len())

		om.HasRemove("abc123")
		om.HasRemove("bar")
		om.HasRemove("foo")
		om.HasRemove("abc-000")

		assert.Equal(t, 0, om.Len())
	})
}

func TestOrderedMap_ForEach(t *testing.T) {
	t.Run("iterate over an empty map", func(t *testing.T) {
		iterations := 0
		om := orderedmap.NewOrderedMap[string, string]()
		om.ForEach(func(k string, v string, order int) {
			iterations++
		})
		assert.Equal(t, 0, iterations)
	})

	t.Run("iterate over values in map", func(t *testing.T) {
		iterations := 0
		var keyOrder []string

		om := orderedmap.NewOrderedMap[string, string]()
		om.Set("foo", "1")
		om.Set("bar", "2")
		om.Set("baz", "5")
		om.Set("123abc", "444")
		om.Set("abc", "123")
		om.Set("abc", "124")
		om.Set("abc123", "321")
		om.Set("abc-000", "000abc")

		om.ForEach(func(k string, v string, order int) {
			iterations++
			keyOrder = append(keyOrder, k)
		})

		assert.Equal(t, 7, iterations)
		assert.Equal(t, 7, len(keyOrder))
	})
}

func TestOrderedMap_ForEachUntil(t *testing.T) {
	t.Run("find first by value and key", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Set(i, fmt.Sprintf("%d", i))
		}

		var foundValue string
		var foundKey int
		var foundOrder int
		om.Transform(func(key int, value string, order int) string {
			return "prefix-" + value
		}).ForEachUntil(func(key int, value string, order int) bool {
			if key == 567 && value == "prefix-567" {
				foundValue = value
				foundKey = key
				foundOrder = order
				return false
			}

			return true
		})

		assert.Equal(t, 567, foundKey)
		assert.Equal(t, "prefix-567", foundValue)
		assert.Equal(t, 0, foundOrder)
	})
}

func TestOrderedMap_Map(t *testing.T) {
	t.Run("map over an empty ordered map", func(t *testing.T) {
		iterations := 0
		om := orderedmap.NewOrderedMap[string, float64]()
		nom := om.Transform(func(k string, v float64, order int) float64 {
			iterations++
			return v
		})

		assert.Equal(t, 0, iterations)
		assert.Equal(t, 0, nom.Len())
	})

	t.Run("map over an ordered map and increment all values", func(t *testing.T) {
		iterations := 0
		var keyOrder []string

		om := orderedmap.NewOrderedMap[string, float64]()
		om.Set("foo", 1)
		om.Set("bar", 2.4)
		om.Set("baz", 5.7)
		om.SetNX("123abc", 444)
		om.Set("abc", 123.99)
		om.Set("abc", 124.88)
		om.SetNX("abc123", 321.4)
		om.Set("abc-000", 0)

		nom := om.Transform(func(k string, v float64, order int) float64 {
			iterations++
			keyOrder = append(keyOrder, k)
			return v + 1
		})

		assert.Equal(t, 7, iterations)
		assert.Equal(t, 7, len(keyOrder))
		assert.Equal(t, []string{"foo", "bar", "baz", "123abc", "abc", "abc123", "abc-000"}, keyOrder)
		assert.Equal(t, 7, nom.Len())

		abcValue, ok := nom.HasGet("abc")
		assert.True(t, ok)
		assert.Equal(t, 125.88, abcValue)

		bazValue, ok := nom.HasGet("baz")
		assert.True(t, ok)
		assert.Equal(t, 6.7, bazValue)
	})
}

func TestOrderedMap_Filter(t *testing.T) {
	t.Run("filter an empty ordered map will result in an empty map", func(t *testing.T) {
		iterations := 0
		om := orderedmap.NewOrderedMap[string, float64]()
		nom := om.Filter(func(k string, v float64, order int) bool {
			iterations++
			return true
		})

		assert.Equal(t, 0, iterations)
		assert.Equal(t, 0, nom.Len())
	})

	t.Run("filter an ordered map preserving some values", func(t *testing.T) {
		iterations := 0
		var keyOrder []string

		om := orderedmap.NewOrderedMap[string, float64]()
		om.Set("foo", 1)
		om.Set("bar", 2.4)
		om.Set("baz", 5.7)
		om.Set("123abc", 444)
		om.Set("abc", 123.99)
		om.Set("abc", 124.88)
		om.Set("abc123", 321.4)
		om.SetNX("abc-000", 0)

		// keep all values less than 100
		nom := om.Filter(func(k string, v float64, order int) bool {
			iterations++
			keyOrder = append(keyOrder, k)
			return v < 100
		})

		assert.Equal(t, 7, iterations)
		assert.Equal(t, 7, len(keyOrder))
		assert.Equal(t, []string{"foo", "bar", "baz", "123abc", "abc", "abc123", "abc-000"}, keyOrder)

		assert.Equal(t, 4, nom.Len())

		abcValue, ok := nom.HasGet("abc")
		assert.False(t, ok)
		assert.Equal(t, float64(0), abcValue)

		bazValue, ok := nom.HasGet("baz")
		assert.True(t, ok)
		assert.Equal(t, 5.7, bazValue)

		barValue, ok := nom.HasGet("bar")
		assert.True(t, ok)
		assert.Equal(t, 2.4, barValue)
	})
}

func Test_Sort(t *testing.T) {
	t.Run("sort and test take top 10 elements", func(t *testing.T) {
		om := orderedmap.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Set(i, fmt.Sprintf("%d", i))
		}

		filterEventNumbers := func(key int, value string, order int) bool {
			return key%2 == 0
		}

		sorter := func(a utils.Pair[int, string], b utils.Pair[int, string]) bool {
			return a.Key < b.Key // reverse
		}

		result := om.Filter(filterEventNumbers).SortBy(sorter)

		require.Equal(t, 500, result.Len())

		expKey := 0
		result.ForEach(func(key int, value string, order int) {
			assert.Equal(t, expKey, key)
			expKey += 2 // only event numbers should have remained
		})
	})
}
