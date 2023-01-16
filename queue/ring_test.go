package queue_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/denismitr/dataflow/queue"
	"github.com/stretchr/testify/assert"
)

func TestAtomicRingQueue_Errors(t *testing.T) {
	t.Run("it will return error on queue overflow", func(t *testing.T) {
		q := queue.NewAtomicRingQueue[string](3)
		if err := q.Enqueue("foo"); err != nil {
			t.Fatalf("could not enqueue foo: %s", err.Error())
		}

		if err := q.Enqueue("bar"); err != nil {
			t.Fatalf("could not enqueue bar: %s", err.Error())
		}

		if q.Len() != 2 {
			t.Fatalf("expected queue len to be 2, got %d", q.Len())
		}

		if err := q.Enqueue("baz"); err != nil {
			t.Fatalf("could not enqueue baz: %s", err.Error())
		}

		if err := q.Enqueue("overflow"); err == nil {
			t.Errorf("should have been an overflow")
		} else if !errors.Is(err, queue.ErrOverflow) {
			t.Errorf("should have been ErrOverflow error, got %+v", err)
		}
	})

	t.Run("it will return an error on empty queue", func(t *testing.T) {
		q := queue.NewAtomicRingQueue[string](3)
		if err := q.Enqueue("foo"); err != nil {
			t.Fatalf("could not enqueue foo: %s", err.Error())
		}

		if err := q.Enqueue("bar"); err != nil {
			t.Fatalf("could not enqueue bar: %s", err.Error())
		}

		if v, err := q.Dequeue(); err != nil {
			t.Fatalf("could not dequeue value")
		} else if v != "foo" {
			t.Fatalf("expected v to be 'foo', got %s", v)
		}

		if v, err := q.Dequeue(); err != nil {
			t.Fatalf("could not dequeue value")
		} else if v != "bar" {
			t.Fatalf("expected v to be 'bar', got %s", v)
		}

		if v, err := q.Dequeue(); err == nil || v != "" {
			t.Errorf("expected err==nil and v=='' got err==%+v and v=='%s'", err, v)
		}
	})
}

func TestRingQueue_EnqueueDequeuePeak(t *testing.T) {
	t.Run("correct order on enqueue and dequeue", func(t *testing.T) {
		q := queue.NewAtomicRingQueue[int](3)
		// enqueue
		assert.NoError(t, q.Enqueue(1))
		assert.NoError(t, q.Enqueue(5))
		assert.NoError(t, q.Enqueue(8))

		assert.Equal(t, 3, q.Len())
		assert.False(t, q.IsEmpty())

		// enqueue should not be possible anymore
		{
			err := q.Enqueue(10)
			assert.Error(t, err)
			assert.True(t, errors.Is(err, queue.ErrOverflow))
		}
		{
			err := q.Enqueue(12)
			assert.Error(t, err)
			assert.True(t, errors.Is(err, queue.ErrOverflow))
		}

		assert.Equal(t, 3, q.Len())
		assert.False(t, q.IsEmpty())

		// dequeue
		{
			v, err := q.Dequeue()
			assert.NoError(t, err)
			assert.Equal(t, 1, v)
		}
		{
			v, err := q.Dequeue()
			assert.NoError(t, err)
			assert.Equal(t, 5, v)
		}
		{
			v, err := q.Dequeue()
			assert.NoError(t, err)
			assert.Equal(t, 8, v)
		}

		// dequeue should not be possible anymore
		{
			v, err := q.Dequeue()
			assert.Error(t, err)
			assert.True(t, errors.Is(err, queue.ErrEmpty))
			assert.Equal(t, 0, v)
		}

		assert.Equal(t, 0, q.Len())
		assert.True(t, q.IsEmpty())
	})
}

func TestAtomicRingQueue_Concurrency(t *testing.T) {
	t.Run("no race should be detected", func(t *testing.T) {
		q := queue.NewAtomicRingQueue[int](50)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1_000; j++ {
					_ = q.Enqueue(j)
				}
			}()
		}

		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				time.Sleep(5 * time.Millisecond)
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1_000; j++ {
					_, _ = q.Dequeue()
				}
			}()
		}

		wg.Wait()
		t.Log("no race should have beem detected")
		if q.Len() != 0 {
			t.Errorf("expected q length to be 0, got %d", q.Len())
		}
	})
}
