package gs

import (
	"golang.org/x/exp/constraints"
)

type OrderedFilterFn[K constraints.Ordered, V any] func(key K, value V, order int) bool

type OrderedMapStream[K constraints.Ordered, V any] struct {
	om          *OrderedMap[K, V]
	concurrency uint8
	filterQueue []OrderedFilterFn[K, V]
}

func (s *OrderedMapStream[K, V]) Run() (*OrderedMap[K, V], error) {
	allPairs := make(chan OrderedPair[K, V], s.concurrency)
	resultCh := make(chan OrderedPair[K, V])

	go func() {
		for p := range s.om.Pairs(true) {
			allPairs <- p
		}
	}()

	for i := 0; i < int(s.concurrency); i++ {
		go func() {
			for pair := range allPairs {
				if s.filterQueue[0](pair.Key, pair.Value, pair.Order) {
					resultCh <- pair
				}
			}
		}()
	}

	var pairSlice []OrderedPair[K, V]
	for result := range resultCh {
		pairSlice = append(pairSlice, result)
	}

	// sort
	return fromOrderedPairs(pairSlice), nil
}
