package gs_test

import (
	"context"
	"fmt"
	"sync/atomic"
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
		result, err := om.Stream(gs.Concurrency(100)).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key%2 > 0
		}).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key > 50
		}).PipeToOrderedMap(context.TODO())

		elapsed := time.Since(start)
		t.Logf("\n\nFilter twice stream with concurrency 100 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, 25, result.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)
	})
}

func TestStream_FilterMapAndForEach(t *testing.T) {
	t.Run("filter and mao with common concurrency of 50", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 100; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		start := time.Now()
		result, err := om.Stream(gs.Concurrency(50)).Filter(func(key int, value string, order int) bool {
			time.Sleep(100 * time.Microsecond)
			return key%2 > 0
		}).Map(func(key int, value string, order int) string {
			return value + "-mapped"
		}).PipeToOrderedMap(context.TODO())

		elapsed := time.Since(start)
		t.Logf("\n\nFilter and Map stream with concurrency 50 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, 50, result.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)

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
		result, err := om.Stream(gs.Concurrency(20)).Filter(func(key int, value string, order int) bool {
			time.Sleep(300 * time.Microsecond)
			return key%2 > 0
		}).Map(func(key int, value string, order int) string {
			return value + "-mapped"
		}).Take(4).PipeToOrderedMap(ctx)

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

	t.Run("filter and map with common concurrency of 100 and forEach with 50", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		var forEachCounter uint64

		start := time.Now()
		result, err := om.Stream(gs.Concurrency(100)).Filter(func(key int, value string, order int) bool {
			time.Sleep(20 * time.Millisecond)
			return key%2 > 0
		}).Map(func(key int, value string, order int) string {
			time.Sleep(20 * time.Millisecond)
			return value + "-mapped"
		}).ForEach(func(key int, value string, order int) {
			time.Sleep(10 * time.Millisecond)
			atomic.AddUint64(&forEachCounter, 1)
		}, gs.Concurrency(50)).PipeToOrderedMap(context.TODO())

		elapsed := time.Since(start)
		t.Logf("\n\nFilter, Map and Iterate stream with common concurrency 100 and 50 in forEach. Elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, 500, result.Len())
		durationIsLess(t, elapsed, 400*time.Millisecond)

		checked := 0
		result.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, result.Len(), checked)
		assert.Equal(t, uint64(500), forEachCounter)
	})
}

func Test_Reduce(t *testing.T) {
	t.Run("simple reduce to struct", func(t *testing.T) {
		type reducable struct {
			a int
			b int
		}

		type result struct {
			aSum    int
			bSum    int
			allSums int
		}

		om := gs.NewOrderedMap[string, reducable]()
		for i := 0; i < 10; i++ {
			om.Put(fmt.Sprintf("key_%d", i), reducable{a: i, b: i + 1})
		}

		stream := gs.NewReducableOrderedMapStream[string, reducable, result](om)

		finalResult, err := stream.Reduce(
			context.TODO(),
			func(carry result, k string, v reducable, order int) result {
				carry.aSum = carry.aSum + v.a
				carry.bSum = carry.bSum + v.b
				carry.allSums = carry.allSums + carry.aSum + carry.bSum
				return carry
			})

		require.NoError(t, err)
		assert.Equal(t, 45, finalResult.aSum)
		assert.Equal(t, 55, finalResult.bSum)
		assert.Equal(t, 385, finalResult.allSums)
	})

	t.Run("reduce to same type as value after map", func(t *testing.T) {
		om := gs.NewOrderedMap[string, int]()
		for i := 0; i < 10_000; i++ {
			om.Put(fmt.Sprintf("key_%d", i), i)
		}

		stream := gs.NewReducableOrderedMapStream[string, int, int](om)

		reducedValue, err := stream.Map(func(key string, value int, order int) int {
			time.Sleep(100 * time.Millisecond)
			return value * 2
		}, gs.Concurrency(500)).Filter(
			func(key string, value int, order int) bool {
				return value < 10
			},
			gs.Concurrency(20),
		).Reduce(context.TODO(), func(carry int, key string, value int, order int) int {
			return carry + value
		})

		require.NoError(t, err)
		assert.Equal(t, 20, reducedValue)
	})
}

func TestOrderedMapStream_First(t *testing.T) {
	t.Run("one threaded find by value", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		stream := gs.NewOrderedMapStream(om)

		first, err := stream.Map(func(key int, value string, order int) string {
			return "prefix-" + value
		}).First(context.TODO(), func(key int, value string, order int) (bool, error) {
			if key == 567 && value == "prefix-567" {
				return true, nil
			}

			return false, nil
		})

		require.NoError(t, err)
		assert.Equal(t, 567, first.Key)
		assert.Equal(t, "prefix-567", first.Value)
		assert.Equal(t, 567, first.Order)
	})

	t.Run("multi threaded threaded find by value", func(t *testing.T) {
		om := gs.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Put(i, fmt.Sprintf("%d", i))
		}

		stream := gs.NewOrderedMapStream(om, gs.Concurrency(50))

		first, err := stream.Map(func(key int, value string, order int) string {
			time.Sleep(50 * time.Millisecond)
			return "prefix-" + value
		}).First(context.TODO(), func(key int, value string, order int) (bool, error) {
			if key == 567 && value == "prefix-567" {
				return true, nil
			}

			return false, nil
		})

		require.NoError(t, err)
		assert.Equal(t, 567, first.Key)
		assert.Equal(t, "prefix-567", first.Value)
		assert.Equal(t, 567, first.Order)
	})
}

func durationIsLess(t *testing.T, a, b time.Duration) {
	t.Helper()

	assert.Truef(t, a < b, "%d is not less than %d", a, b)
}
