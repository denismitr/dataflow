package list_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/denismitr/dataflow/list"
)

func Test_MapReduce(t *testing.T) {
	t.Run("concurrency with no errors", func(t *testing.T) {
		const n = 10_000
		var exp int
		type acc struct {
			maxIndex int
			value    int
		}
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			exp += i * 2
			in = append(in, i)
		}

		mapper := func(idx int, value int) (int, error) {
			return value * 2, nil
		}

		reducer := func(acc acc, idx int, value int) (acc, error) {
			acc.value += value
			if idx > acc.maxIndex {
				acc.maxIndex = idx
			}
			return acc, nil
		}

		result, err := list.MapReduce(
			context.TODO(),
			in,
			mapper,
			reducer,
			acc{},
			list.WithConcurrency(10),
		)
		if err != nil {
			t.Fatal(err)
		}

		if result.value != exp {
			t.Fatalf("expected result to be %d, got %d", exp, result.value)
		}

		if result.maxIndex != 9_999 {
			t.Fatalf("expected result to be %d, got %d", 9_999, result.maxIndex)
		}
	})

	t.Run("all mappers return an error", func(t *testing.T) {
		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(idx int, value int) (int, error) {
			return value, fmt.Errorf("all are errors")
		}

		reducer := func(acc int, idx int, value int) (int, error) {
			acc += value
			return acc, nil
		}

		result, err := list.MapReduce(
			context.TODO(),
			in,
			mapper,
			reducer,
			0,
			list.WithConcurrency(10),
		)
		if err == nil {
			t.Fatal("expected to get an error")
		}

		if result != 0 {
			t.Fatalf("expected result to be 0, got %d", result)
		}
	})

	t.Run("filter", func(t *testing.T) {
		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(idx int, value int) (int, error) {
			if idx%2 != 0 {
				return 0, list.ErrSkip
			}
			return value, nil
		}

		reducer := func(acc []int, idx int, value int) ([]int, error) {
			return append(acc, value), nil
		}

		concurrency := list.WithConcurrency(10)
		init := make([]int, 0, n/2)
		result, err := list.MapReduce(context.TODO(), in, mapper, reducer, init, concurrency)
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

	t.Run("mapper errors below the threshold", func(t *testing.T) {
		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(idx int, value int) (int, error) {
			if value == 5_000 {
				return 0, fmt.Errorf("value is 5000")
			}

			return value, nil
		}

		reducer := func(acc int, idx int, value int) (int, error) {
			acc += value
			return acc, nil
		}

		result, err := list.MapReduce(
			context.TODO(),
			in,
			mapper,
			reducer,
			0,
			list.WithConcurrency(100),
			list.ErrorThreshold(2),
		)
		if err == nil {
			t.Fatal("expected to get an error")
		}

		assert.Equal(t, "1 map reduce errors: value is 5000", err.Error())

		if result != 49990000 {
			t.Fatalf("expected result to be 49990000, got %d", result)
		}
	})

	t.Run("mapper errors over the threshold", func(t *testing.T) {
		const n = 10_000
		in := make([]int, 0, n)
		for i := 0; i < n; i++ {
			in = append(in, i)
		}

		mapper := func(idx int, value int) (int, error) {
			if value == 5_000 {
				return 0, fmt.Errorf("value is 5000")
			}

			if value == 6_000 {
				return 0, fmt.Errorf("value is 6000")
			}

			return value, nil
		}

		reducer := func(acc int, idx int, value int) (int, error) {
			acc += value
			return acc, nil
		}

		result, err := list.MapReduce(
			context.TODO(),
			in,
			mapper,
			reducer,
			0,
			list.WithConcurrency(3),
			list.ErrorThreshold(2),
		)
		if err == nil {
			t.Fatal("expected an error")
		}

		assert.Equal(t, "2 map reduce errors: value is 5000, value is 6000", err.Error())

		if result == 0 {
			t.Fatalf("expected some result to have been processed")
		}
	})
}
