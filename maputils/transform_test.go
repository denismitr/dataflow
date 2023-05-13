package maputils

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTransform(t *testing.T) {
	t.Run("empty map should return empty map", func(t *testing.T) {
		in := map[int]string{}
		out := Transform(in, func(k int, v string) string { return "foo" })
		assert.Equal(t, map[int]string{}, out)
	})

	t.Run("non empty map values should be transformed in a new map", func(t *testing.T) {
		in := map[int]string{1: "foo", 3: "bar", 2: "baz"}
		out := Transform(in, func(k int, v string) string {
			return fmt.Sprintf("%s-transformed", v)
		})
		assert.Equal(t, map[int]string{
			1: "foo-transformed",
			3: "bar-transformed",
			2: "baz-transformed",
		}, out)
	})
}

func TestTransformValuesAsync(t *testing.T) {
	t.Run("non empty map values should be transformed in a new map", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		in := map[int]string{1: "foo", 3: "bar", 2: "baz"}
		out, err := TransformAsync(ctx, in, func(ctx context.Context, k int, v string) (string, error) {
			time.Sleep(100 * time.Millisecond)
			return fmt.Sprintf("%s-transformed", v), nil
		}, 5)

		require.NoError(t, err)
		assert.Equal(t, map[int]string{
			1: "foo-transformed",
			3: "bar-transformed",
			2: "baz-transformed",
		}, out)
	})

	t.Run("it will return on context exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		in := map[int]string{1: "foo", 3: "bar", 2: "baz"}
		out, err := TransformAsync(ctx, in, func(ctx context.Context, k int, v string) (string, error) {
			time.Sleep(3 * time.Second)
			return fmt.Sprintf("%s-transformed", v), nil
		}, 3)

		require.Error(t, err)
		require.Nil(t, out)
	})
}

func TestTransformWithKeys(t *testing.T) {
	t.Run("empty map should return empty map", func(t *testing.T) {
		in := map[int]string{}
		out := TransformWithKeys(in, func(k int, v string) (int, string) { return k, "foo" })
		assert.Equal(t, map[int]string{}, out)
	})

	t.Run("non empty map values should be transformed in a new map", func(t *testing.T) {
		in := map[int]string{1: "foo", 3: "bar", 2: "baz"}
		out := TransformWithKeys(in, func(k int, v string) (int, string) {
			return k, fmt.Sprintf("%s-transformed", v)
		})
		assert.Equal(t, map[int]string{
			1: "foo-transformed",
			3: "bar-transformed",
			2: "baz-transformed",
		}, out)
	})

	t.Run("keys and values can be transformed in a new map", func(t *testing.T) {
		in := map[int]string{1: "foo", 3: "bar", 2: "baz"}
		out := TransformWithKeys(in, func(k int, v string) (int, string) {
			return k + 10, fmt.Sprintf("%s-transformed", v)
		})
		assert.Equal(t, map[int]string{
			11: "foo-transformed",
			13: "bar-transformed",
			12: "baz-transformed",
		}, out)
	})
}
