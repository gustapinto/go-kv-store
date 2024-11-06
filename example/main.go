package main

import (
	"log"
	"time"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	store, err := gokvstore.NewStore(".", "store.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	partition, err := store.Partition("partition-1")
	if err != nil {
		log.Fatal(err)
	}
	defer partition.Close()

	if err := partition.Put([]byte("hello"), []byte("world!")); err != nil {
		log.Fatal(err)
	}

	if err := partition.Put([]byte("foo"), []byte("foobar, man this example text is old")); err != nil {
		log.Fatal(err)
	}

	if err := partition.Put([]byte("hello"), []byte("world! now updated!")); err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	helloValue, err := partition.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found hello:", string(helloValue))

	if err := partition.Delete([]byte("hello")); err != nil {
		log.Fatal(err)
	}

	log.Println("Deleted hello")

	barValue, err := partition.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found foo:", string(barValue))

	time.Sleep(5 * time.Second)
}
