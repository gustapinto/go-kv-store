package main

import (
	"log"
	"time"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	store, err := gokvstore.NewLocalStorageStore(".")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	partition, err := store.Partition("part-1")
	if err != nil {
		log.Fatal(err)
	}
	defer partition.Close()

	if err := partition.PUT("hello", "world"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	v, err := partition.GET("hello")
	if err != nil {
		log.Fatal(err)
	}

	log.Println(v)

	time.Sleep(5 * time.Second)
}
