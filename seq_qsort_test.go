package main

import (
	"fmt"
	"testing"
	"time"
)

func seqQuickSort(arr []int) []int {
	if len(arr) <= 1 {
		return arr
	}

	pivot := arr[len(arr)-1]
	var left, right []int

	for _, num := range arr[:len(arr)-1] {
		if num < pivot {
			left = append(left, num)
		} else {
			right = append(right, num)
		}
	}

	return append(append(seqQuickSort(left), pivot), seqQuickSort(right)...)
}

func TestSeqQuickSortPerformance(t *testing.T) {
	const NumTests = 5
	const ArrSize = 100000000

	arr := generateRandomArray(ArrSize)
	var durations []time.Duration
	var totalDuration time.Duration

	for i := 0; i < NumTests; i++ {
		start := time.Now()
		sortedArr := seqQuickSort(arr)
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
