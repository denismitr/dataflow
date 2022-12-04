package keyvalue_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/denismitr/dataflow/keyvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_Filter(t *testing.T) {
	t.Run("concurrency 100", func(t *testing.T) {
		om := keyvalue.NewOrderedMap[int, string]()
		for i := 0; i < 100; i++ {
			om.Set(i, fmt.Sprintf("%d", i))
		}

		dst := keyvalue.NewOrderedMap[int, string]()
		start := time.Now()
		err := om.Stream(keyvalue.Concurrency(100)).
			Filter(func(ctx context.Context, key int, value string) (bool, error) {
				time.Sleep(100 * time.Microsecond)
				return key%2 > 0, nil
			}).Filter(func(ctx context.Context, key int, value string) (bool, error) {
			time.Sleep(100 * time.Microsecond)
			return key > 50, nil
		}).PipeInto(context.TODO(), dst)

		elapsed := time.Since(start)
		t.Logf("\n\nFilter twice stream with concurrency 100 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		assert.Equal(t, 25, dst.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)
	})
}

func TestStream_FilterMapTakeAndForEach(t *testing.T) {
	t.Run("filter and map with common concurrency of 50", func(t *testing.T) {
		om := keyvalue.NewOrderedMap[int, string]()
		for i := 0; i < 100; i++ {
			om.Set(i, fmt.Sprintf("%d", i))
		}

		f := func(ctx context.Context, key int, value string) (bool, error) {
			time.Sleep(100 * time.Microsecond)
			return key%2 > 0, nil
		}

		m := func(ctx context.Context, key int, value string) (string, error) {
			return value + "-mapped", nil
		}

		start := time.Now()
		dst := keyvalue.NewOrderedMap[int, string]()
		err := om.
			Stream(keyvalue.Concurrency(50)).
			Filter(f).Map(m).PipeInto(context.TODO(), dst)

		elapsed := time.Since(start)
		t.Logf("\n\nFilter and Map stream with concurrency 50 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.Equal(t, 50, dst.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)

		checked := 0
		dst.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, dst.Len(), checked)
	})

	t.Run("concurrency 20 and take 4 at the end", func(t *testing.T) {
		om := keyvalue.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.SetNX(i, fmt.Sprintf("%d", i))
		}

		require.Equal(t, 1000, om.Len())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		f := func(ctx context.Context, key int, value string) (bool, error) {
			time.Sleep(300 * time.Microsecond)
			return key%2 > 0, nil
		}

		m := func(ctx context.Context, key int, value string) (string, error) {
			return value + "-mapped", nil
		}

		dst := keyvalue.NewOrderedMap[int, string]()
		start := time.Now()
		err := om.Stream(keyvalue.Concurrency(20)).
			Filter(f).
			Map(m).
			Take(4).
			PipeInto(ctx, dst)

		elapsed := time.Since(start)
		t.Logf("\n\nFilter and Map stream with concurrency 50 and take 4 elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.Equal(t, 4, dst.Len())
		durationIsLess(t, elapsed, 40*time.Millisecond)

		checked := 0
		dst.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, dst.Len(), checked)
	})

	t.Run("filter and map with common concurrency of 100 and forEach with 50", func(t *testing.T) {
		om := keyvalue.NewOrderedMap[int, string]()
		for i := 0; i < 1_000; i++ {
			om.Set(i, fmt.Sprintf("%d", i))
		}

		var forEachCounter uint64

		f := func(ctx context.Context, key int, value string) (bool, error) {
			time.Sleep(20 * time.Millisecond)
			return key%2 > 0, nil
		}

		m := func(ctx context.Context, key int, value string) (string, error) {
			time.Sleep(20 * time.Millisecond)
			return value + "-mapped", nil
		}

		fr := func(ctx context.Context, key int, value string) error {
			time.Sleep(10 * time.Millisecond)
			atomic.AddUint64(&forEachCounter, 1)
			return nil
		}

		dst := keyvalue.NewOrderedMap[int, string]()
		start := time.Now()
		err := om.
			Stream(keyvalue.Concurrency(100)).
			Filter(f).
			Map(m).
			ForEach(fr, keyvalue.Concurrency(50)).
			PipeInto(context.TODO(), dst)

		elapsed := time.Since(start)
		t.Logf("\n\nFilter, Map and Iterate stream with common concurrency 100 and 50 in forEach. Elapsed in %s", elapsed.String())

		require.NoError(t, err)
		require.Equal(t, 500, dst.Len())
		durationIsLess(t, elapsed, 400*time.Millisecond)

		checked := 0
		dst.ForEach(func(key int, value string, order int) {
			assert.Equal(t, fmt.Sprintf("%d-mapped", key), value)
			checked++
		})
		assert.Equal(t, dst.Len(), checked)
		assert.Equal(t, uint64(500), forEachCounter)
	})
}



func durationIsLess(t *testing.T, a, b time.Duration) {
	t.Helper()

	assert.Truef(t, a < b, "%d is not less than %d", a, b)
}
