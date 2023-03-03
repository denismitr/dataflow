package superstream_test

import (
	"context"
	"fmt"
	stream "github.com/denismitr/dataflow/stream"
	"strconv"
	"testing"
)

func Test_MapReduce(t *testing.T) {
	t.Run("int slice concurrent map reduce with no errors", func(t *testing.T) {
		const n = 10_000
		var valueExp int
		var indexExp int = n - 1

		type acc struct {
			maxIndex int
			value    int
		}
		type intSliceItem = stream.Item[int, int]

		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			valueExp += i * 2
			in = append(in, i)
		}

		mapper := func(_ context.Context, item intSliceItem) (intSliceItem, error) {
			return intSliceItem{Key: item.Key, Value: item.Value * 2}, nil
		}

		reducer := func(_ context.Context, acc acc, item intSliceItem) (acc, error) {
			acc.value += item.Value
			if item.Key > acc.maxIndex {
				acc.maxIndex = item.Key
			}
			return acc, nil
		}

		result, err := stream.MapReduce(
			context.TODO(),
			stream.Slice(in),
			mapper,
			reducer,
			acc{},
			stream.WithConcurrency(10),
		)
		if err != nil {
			t.Fatal(err)
		}

		if result.value != valueExp {
			t.Fatalf("expected result to be %d, got %d", valueExp, result.value)
		}

		if result.maxIndex != indexExp {
			t.Fatalf("expected result to be %d, got %d", indexExp, result.maxIndex)
		}
	})

	t.Run("int[string] map concurrent map reduce with no errors", func(t *testing.T) {
		const n = 10_000
		var oddCountExp = n / 2
		var indexExp = n - 1

		type acc struct {
			maxIndex int
			oddCount int
		}

		type intStringMapItem = stream.Item[int, string]
		type intIntMapItem = stream.Item[int, int]

		in := make(map[int]string, n)
		for i := 0; i < n; i++ {
			in[i] = fmt.Sprintf("%d", i)
		}

		mapper := func(_ context.Context, item intStringMapItem) (intIntMapItem, error) {
			var newValue int
			if item.Key%2 == 0 {
				newValue = 0
			} else {
				newValue, _ = strconv.Atoi(item.Value)
			}
			return intIntMapItem{Key: item.Key, Value: newValue}, nil
		}

		reducer := func(_ context.Context, acc acc, item intIntMapItem) (acc, error) {
			if item.Value != 0 {
				acc.oddCount += 1
			}

			if item.Key > acc.maxIndex {
				acc.maxIndex = item.Key
			}

			return acc, nil
		}

		result, err := stream.MapReduce(
			context.TODO(),
			stream.Map(in),
			mapper,
			reducer,
			acc{},
			stream.WithConcurrency(10),
			stream.ErrorThreshold(1),
		)
		if err != nil {
			t.Fatal(err)
		}

		if result.oddCount != oddCountExp {
			t.Fatalf("expected odd count to be %d, got %d", oddCountExp, result.oddCount)
		}

		if result.maxIndex != indexExp {
			t.Fatalf("expected result to be %d, got %d", indexExp, result.maxIndex)
		}
	})

	t.Run("all mappers return an error", func(t *testing.T) {
		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(_ context.Context, item stream.Item[int, int]) (stream.Item[int, int], error) {
			return stream.Item[int, int]{}, fmt.Errorf("some error in mapper")
		}

		reducer := func(_ context.Context, acc int, item stream.Item[int, int]) (int, error) {
			acc += item.Value
			return acc, nil
		}

		result, err := stream.MapReduce(
			context.TODO(),
			stream.Slice(in),
			mapper,
			reducer,
			0,
			stream.WithConcurrency(10),
		)
		if err == nil {
			t.Fatal("expected to get an error")
		}

		if result != 0 {
			t.Fatalf("expected result to be 0, got %d", result)
		}
	})

	t.Run("filter", func(t *testing.T) {
		type intSliceItem = stream.Item[int, int]

		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(_ context.Context, item intSliceItem) (intSliceItem, error) {
			if item.Key%2 != 0 {
				return stream.Zero[intSliceItem](), stream.ErrSkip
			}
			return item, nil
		}

		reducer := func(_ context.Context, acc []int, item intSliceItem) ([]int, error) {
			return append(acc, item.Value), nil
		}

		concurrency := stream.WithConcurrency(10)
		init := make([]int, 0, n/2)
		ctx := context.TODO()
		result, err := stream.MapReduce(ctx, stream.Slice(in), mapper, reducer, init, concurrency)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		{
			want, have := n/2, len(result)
			if want != have {
				t.Fatalf("expected resulting slice to have len %d, got %d", want, have)
			}
		}

		{
			for i := range result {
				if result[i]%2 != 0 {
					t.Fatalf("did not expect to see an odd number %d", result[i])
				}
			}
		}
	})
	//
	//t.Run("mapper errors below the threshold", func(t *testing.T) {
	//	const n = 10_000
	//	in := make([]int, 0, n)
	//	for i := 0; i < n; i++ {
	//		in = append(in, i)
	//	}
	//
	//	mapper := func(idx int, value int) (int, error) {
	//		if value == 5_000 {
	//			return 0, fmt.Errorf("value is 5000")
	//		}
	//
	//		return value, nil
	//	}
	//
	//	reducer := func(acc int, idx int, value int) (int, error) {
	//		acc += value
	//		return acc, nil
	//	}
	//
	//	result, err := list.MapReduce(
	//		context.TODO(),
	//		in,
	//		mapper,
	//		reducer,
	//		0,
	//		list.WithConcurrency(100),
	//		list.ErrorThreshold(2),
	//	)
	//	if err == nil {
	//		t.Fatal("expected to get an error")
	//	}
	//
	//	assert.Equal(t, "1 map reduce errors: value is 5000", err.Error())
	//
	//	if result != 49990000 {
	//		t.Fatalf("expected result to be 49990000, got %d", result)
	//	}
	//})
	//
	//t.Run("mapper errors over the threshold", func(t *testing.T) {
	//	const n = 10_000
	//	in := make([]int, 0, n)
	//	for i := 0; i < n; i++ {
	//		in = append(in, i)
	//	}
	//
	//	mapper := func(idx int, value int) (int, error) {
	//		if value == 5_000 {
	//			return 0, fmt.Errorf("value is 5000")
	//		}
	//
	//		if value == 6_000 {
	//			return 0, fmt.Errorf("value is 6000")
	//		}
	//
	//		return value, nil
	//	}
	//
	//	reducer := func(acc int, idx int, value int) (int, error) {
	//		acc += value
	//		return acc, nil
	//	}
	//
	//	result, err := list.MapReduce(
	//		context.TODO(),
	//		in,
	//		mapper,
	//		reducer,
	//		0,
	//		list.WithConcurrency(3),
	//		list.ErrorThreshold(2),
	//	)
	//	if err == nil {
	//		t.Fatal("expected an error")
	//	}
	//
	//	assert.Equal(t, "2 map reduce errors: value is 5000, value is 6000", err.Error())
	//
	//	if result == 0 {
	//		t.Fatalf("expected some result to have been processed")
	//	}
	//})
}
