package gs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/denismitr/gs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_Filter(t *testing.T) {
	t.Run("concurrency 100", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 100; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		start := time.Now()
		result, err := om.Stream(100).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key%2 > 0
		}).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key > 50
		}).Run(context.TODO())

		elapsed := time.Since(start)
		t.Logf("\n\nFilter twice stream with concurrency 100 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, 25, result.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)
	})
}

func TestStream_FilterAndMap(t *testing.T) {
	t.Run("concurrency 50", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 100; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		start := time.Now()
		result, err := om.Stream(50).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key%2 > 0
		}).Map(func(key int, value string, order int) string {
			return value + "-mapped"
		}).Run(context.TODO())

		elapsed := time.Since(start)
		t.Logf("\n\nFilter and Map stream with concurrency 50 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, 50, result.Len())
		durationIsLess(t, elapsed, 400*time.Millisecond)

		checked := 0
		result.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, result.Len(), checked)
	})

	t.Run("concurrency 20 and take 4 at the end", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		require.Equal(t, 1000, om.Len())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		result, err := om.Stream(20).Filter(func(key int, value string, order int) bool {
			time.Sleep(300 * time.Microsecond)
			return key%2 > 0
		}).Map(func(key int, value string, order int) string {
			return value + "-mapped"
		}).Take(4).Run(ctx)

		elapsed := time.Since(start)
		t.Logf("\n\nFilter and Map stream with concurrency 50 and take 4 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, 4, result.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)

		checked := 0
		result.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, result.Len(), checked)
	})
}

func durationIsLess(t *testing.T, a, b time.Duration) {
	t.Helper()

	assert.Truef(t, a < b, "%d is not less than %d", a, b)
}
