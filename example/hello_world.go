package main

import (
	"fmt"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore("data"))
	if err != nil {
		panic(err)
	}

	if err := collection.Put("1e53ccd4-107f-4491-8b65-bff3e477c201", []byte("Hello World!"), false); err != nil {
		panic(err)
	}

	value, err := collection.Get("1e53ccd4-107f-4491-8b65-bff3e477c201")
	if err != nil {
		panic(err)
	}

	fmt.Println("Found:", string(value))
}
