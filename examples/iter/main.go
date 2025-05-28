package main

import (
	"fmt"
	"time"

	"github.com/gustapinto/go-kv-store/kv"
	"github.com/gustapinto/go-kv-store/kv/catalog"
)

func main() {
	start := time.Now()

	catalog := catalog.NewJSONL("./iter_catalog.jsonl", catalog.Buffered)
	col, err := kv.NewCollection(catalog, kv.LazyLoad)
	if err != nil {
		panic(err)
	}

	for i := range 10 {
		err := col.Set(fmt.Sprintf("KEY_%d", i), []byte("Lorem ipsum dolor sit amet"))
		if err != nil {
			panic(err)
		}
	}

	i := 0
	err = col.Iter(func(key string, value []byte) (shouldContinue bool) {
		if i >= 5 {
			return false // Early stop
		}

		fmt.Printf("(key=%s, value=%s)\n", key, value)
		i++

		return true
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Since(start).Milliseconds(), "ms")
}
