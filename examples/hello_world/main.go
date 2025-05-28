package main

import (
	"fmt"

	kv "github.com/gustapinto/go-kv-store/v2"
	"github.com/gustapinto/go-kv-store/v2/catalog"
)

const (
	_key = "Hello"
)

func main() {
	catalog := catalog.NewLocal("./hello_world_catalog.jsonl", catalog.Sync)
	col, err := kv.NewCollection(catalog, kv.EagerLoad)
	if err != nil {
		panic(err)
	}

	if err = col.Set(_key, []byte("World")); err != nil {
		panic(err)
	}

	val, exists := col.Get(_key)

	fmt.Println(_key, string(val), exists)
}
