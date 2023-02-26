package queue

import (
	"errors"
	"sync"

	"github.com/denismitr/dataflow/utils"
)

var (
	ErrOverflow = errors.New("queue is full")
	ErrEmpty    = errors.New("queue is empty")
)

type RingQueue[T any] struct {
	mux   sync.RWMutex
	head  uint64
	tail  uint64
	size  uint64
	count uint64
	buf   []T
}

func NewAtomicRingQueue[T any](size uint64) *RingQueue[T] {
	return &RingQueue[T]{
		buf:  make([]T, size),
		size: size,
	}
}

func (q *RingQueue[T]) Len() int {
	q.mux.RLock()
	defer q.mux.RUnlock()
	return int(q.count)
}

func (q *RingQueue[T]) IsEmpty() bool {
	q.mux.RLock()
	defer q.mux.RUnlock()
	return q.count == 0
}

func (q *RingQueue[T]) Enqueue(item T) error {
	q.mux.Lock()
	defer q.mux.Unlock()
	if q.count == q.size {
		return ErrOverflow
	}

	q.buf[q.head] = item
	if q.head == q.size-1 {
		q.head = 0
	} else {
		q.head++
	}
	q.count++

	return nil
}

func (q *RingQueue[T]) Peak() (T, error) {
	q.mux.RLock()
	defer q.mux.RUnlock()
	if q.count == 0 {
		return utils.GetZero[T](), ErrEmpty
	}
	result := q.buf[q.tail]
	return result, nil
}

func (q *RingQueue[T]) Dequeue() (T, error) {
	q.mux.Lock()
	defer q.mux.Unlock()
	if q.count == 0 {
		return utils.GetZero[T](), ErrEmpty
	}

	result := q.buf[q.tail]
	if q.tail >= q.size-1 {
		q.tail = 0
	} else {
		q.tail++
	}

	q.count--
	return result, nil
}
