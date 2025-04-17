package main

import (
	"fmt"

	"github.com/google/uuid"
	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore("./data"))
	if err != nil {
		panic(err)
	}

	key := uuid.NewString()

	if err := collection.Put(key, []byte("Hello World!"), false); err != nil {
		panic(err)
	}

	value, err := collection.Get(key)
	if err != nil {
		panic(err)
	}

	fmt.Println("Found:", string(value))

	if err := collection.Delete(key); err != nil {
		panic(err)
	}
}
