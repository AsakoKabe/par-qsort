package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const maxWorkers = 4

type Job func()

type WorkerPool struct {
	jobQueue   chan Job // Каналы задач для разных уровней
	maxWorkers int
}

func NewWorkerPool(maxWorkers int) *WorkerPool {

	pool := &WorkerPool{
		jobQueue:   make(chan Job),
		maxWorkers: maxWorkers,
	}

	// Запускаем воркеров для каждого уровня рекурсии
	for i := 0; i < maxWorkers; i++ {
		go pool.worker()
	}
	return pool
}

func (pool *WorkerPool) worker() {
	for job := range pool.jobQueue {
		job()
	}
}

// Add добавляет задачу в очередь и увеличивает счетчик WaitGroup
func (pool *WorkerPool) Add(job Job) {
	pool.jobQueue <- job
}

// Wait дожидается выполнения всех задач
func (pool *WorkerPool) Wait() {
	close(pool.jobQueue) // Закрываем канал после выполнения всех задач
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

func filter(pool *WorkerPool, a []int, f func(int) bool) []int {
	flags := mapFunc(pool, a, f)
	sums := scan(pool, flags)
	ans := make([]int, sums[len(sums)-1])

	wg := sync.WaitGroup{}
	for i := range a {
		if flags[i] == 1 {
			i := i
			wg.Add(1)
			pool.Add(
				func() {
					defer wg.Done()
					ans[sums[i]] = a[i]
				},
			)
		}
	}
	wg.Wait()
	return ans
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
	const NumTests = 1
	const ArrSize = 1000

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
