package gokvstore

import "math/rand"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ11234567890")

func generateRandomString(size int) string {
	buffer := make([]rune, size)

	for i := range buffer {
		buffer[i] = letters[rand.Intn(len(letters))]
	}

	return string(buffer)
}
