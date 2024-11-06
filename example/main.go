package main

import (
	"log"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	store, err := gokvstore.NewStore("storeDir", "store.db")
	if err != nil {
		log.Fatal(err)
	}

	partition, err := store.Partition("partitionDir", "store.part.db")
	if err != nil {
		log.Fatal(err)
	}

	if err := partition.Put([]byte("hello"), []byte("world!")); err != nil {
		log.Fatal(err)
	}

	if err := store.Put([]byte("foo"), []byte("foobar, man this example text is old")); err != nil {
		log.Fatal(err)
	}

	if err := partition.Put([]byte("hello"), []byte("world! now updated!")); err != nil {
		log.Fatal(err)
	}

	helloValue, err := partition.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found hello:", string(helloValue))

	if err := partition.Delete([]byte("hello")); err != nil {
		log.Fatal(err)
	}

	log.Println("Deleted: hello")

	barValue, err := store.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found foo:", string(barValue))
}
