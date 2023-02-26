package list_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/denismitr/gs/list"
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
			exp += (i * 2)
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

		result, err := list.MapReduce(context.TODO(), in, mapper, reducer, list.WithConcurrency[acc](10))
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

		result, err := list.MapReduce(context.TODO(), in, mapper, reducer, list.WithConcurrency[int](10))
		if err == nil {
			t.Fatal("expected to get an error")
		}

		if result != 0 {
			t.Fatalf("expected result to be 0, got %d", result)
		}
	})
}
