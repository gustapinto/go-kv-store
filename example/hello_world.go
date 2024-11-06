package main

import (
	"fmt"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	store, err := gokvstore.NewStore("data", "store.db")
	if err != nil {
		panic(err)
	}

	if err := store.Put([]byte("1e53ccd4-107f-4491-8b65-bff3e477c201"), []byte("Hello World!")); err != nil {
		panic(err)
	}

	value, err := store.Get([]byte("1e53ccd4-107f-4491-8b65-bff3e477c201"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Found:", string(value))
}
