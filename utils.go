package main

import "math/rand"

func generateRandomArray(size int) []int {
	arr := make([]int, size)
	for i := range arr {
		arr[i] = rand.Int()
	}
	return arr
}

func isSorted(arr []int) bool {
	for i := 1; i < len(arr); i++ {
		if arr[i-1] > arr[i] {
			return false
		}
	}
	return true
}
