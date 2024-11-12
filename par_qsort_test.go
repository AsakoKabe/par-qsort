package main

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const maxWorkers = 4

type Job func()

type WorkerPool struct {
	enqueue func(Job)
	jobs    chan Job
}

func NewWorkerPool(maxWorkers int) *WorkerPool {

	jobs := make(chan Job)
	var enqueue func(Job)

	// workers
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for j := range jobs {
				j()
			}
		}()
	}

	enqueue = func(j Job) {
		select {
		case jobs <- j: // another worker took it
		default: // no free worker; do the job now
			j()
		}
	}

	return &WorkerPool{
		enqueue: enqueue,
		jobs:    jobs,
	}
}

func (pool *WorkerPool) Add(job Job) {
	pool.enqueue(job)
}

func (pool *WorkerPool) Wait() {
	close(pool.jobs)
}

func scan(pool *WorkerPool, arr []int) []int {
	n := len(arr) + 1
	if n == 0 {
		return []int{}
	}

	logN := int(math.Ceil(math.Log2(float64(n))))
	size := 1
	for size < n {
		size *= 2
	}

	c := make([]int, size)
	copy(c, arr)
	wg := sync.WaitGroup{}

	for d := 0; d < logN; d++ {
		step := int(math.Pow(2, float64(d+1)))
		for i := 0; i < size; i += step {
			wg.Add(1)
			i := i
			pool.Add(
				func() {
					defer wg.Done()
					if i+step/2 < size {
						c[i+step-1] += c[i+step/2-1]
					}
				},
			)
		}
		wg.Wait()
	}

	c[size-1] = 0

	for d := logN - 1; d >= 0; d-- {
		step := int(math.Pow(2, float64(d+1)))
		for i := 0; i < size; i += step {
			i := i
			wg.Add(1)
			pool.Add(
				func() {
					defer wg.Done()
					left := c[i+step/2-1]
					c[i+step/2-1] = c[i+step-1]
					c[i+step-1] += left
				},
			)
		}
		wg.Wait()
	}
	return c[:n]
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mapFunc(pool *WorkerPool, a []int, f func(int) bool) []int {
	b := make([]int, len(a))
	wg := sync.WaitGroup{}
	for i := range a {
		i := i
		wg.Add(1)
		pool.Add(
			func() {
				defer wg.Done()
				b[i] = boolToInt(f(a[i]))
			},
		)
	}
	wg.Wait()
	return b
}

// func filter(pool *WorkerPool, a []int, f func(int) bool) []int {
// 	flags := mapFunc(pool, a, f)
// 	sums := scan(pool, flags)
// 	ans := make([]int, sums[len(sums)-1])
//
// 	wg := sync.WaitGroup{}
// 	for i := range a {
// 		if flags[i] == 1 {
// 			i := i
// 			wg.Add(1)
// 			pool.Add(
// 				func() {
// 					defer wg.Done()
// 					ans[sums[i]] = a[i]
// 				},
// 			)
// 		}
// 	}
// 	wg.Wait()
// 	return ans
// }

func filter(pool *WorkerPool, a []int, f func(int) bool) (ret []int) {
	for _, item := range a {
		if f(item) {
			ret = append(ret, item)
		}
	}
	return
}

func parQuickSort(pool *WorkerPool, A []int) []int {
	if len(A) <= 1000 {
		return seqQuickSort(A)
	}

	pivot := A[rand.Intn(len(A))]

	var A1, A2, A3 []int
	wg := sync.WaitGroup{}

	wg.Add(1)
	pool.Add(
		func() {
			defer wg.Done()
			A1 = parQuickSort(pool, filter(pool, A, func(x int) bool { return x < pivot }))
		},
	)

	wg.Add(1)
	pool.Add(
		func() {
			defer wg.Done()
			A2 = filter(pool, A, func(x int) bool { return x == pivot })
		},
	)

	wg.Add(1)
	pool.Add(
		func() {
			defer wg.Done()
			A3 = parQuickSort(pool, filter(pool, A, func(x int) bool { return x > pivot }))
		},
	)

	wg.Wait()
	return append(append(A1, A2...), A3...)
}

func TestParQuickSortPerformance(t *testing.T) {
	const NumTests = 5
	const ArrSize = 100000000
	runtime.GOMAXPROCS(4)

	arr := generateRandomArray(ArrSize)
	var durations []time.Duration
	var totalDuration time.Duration

	for i := 0; i < NumTests; i++ {
		start := time.Now()
		pool := NewWorkerPool(maxWorkers)
		sortedArr := parQuickSort(pool, arr)
		pool.Wait()
		duration := time.Since(start)
		if !isSorted(sortedArr) {
			t.Errorf("Run %d: array is not sorted correctly", i+1)
		}

		durations = append(durations, duration)
		totalDuration += duration
	}

	averageDuration := totalDuration / 5

	fmt.Println("Run\tExecution Time")
	fmt.Println("------------------------")
	for i, duration := range durations {
		fmt.Printf("%d\t%v\n", i+1, duration)
	}
	fmt.Println("------------------------")
	fmt.Printf("Average Execution Time: %v\n", averageDuration)
}
