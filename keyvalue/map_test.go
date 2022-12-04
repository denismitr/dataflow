package keyvalue_test

import (
	"testing"

	"github.com/denismitr/dataflow/keyvalue"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	type structKey struct {
		A int
		B string
	}

	t.Run("map with struct keys", func(t *testing.T) {
		m := map[structKey]int{
			{A: 1, B: "foo"}:  1,
			{A: 30, B: "bar"}: 2,
		}

		result := keyvalue.Map[structKey, int](m, func(key structKey, value int) int {
			if key.A == 1 && key.B == "foo" {
				return (value + value) * 3
			}

			return value - 1
		})

		assert.Equal(t, 6, result[structKey{A: 1, B: "foo"}])
		assert.Equal(t, 6, result[structKey{A: 1, B: "foo"}])
	})
}
