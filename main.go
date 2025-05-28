package main

import (
	"fmt"
	"time"

	"github.com/gustapinto/go-kv-store/kv"
	"github.com/gustapinto/go-kv-store/kv/catalog"
)

func main() {
	start := time.Now()

	catalog := catalog.NewJSONL("./catalog.jsonl", catalog.Buffered)
	col, err := kv.NewCollection(catalog, kv.LazyLoad)
	if err != nil {
		panic(err)
	}

	for i := range 1000 {
		err := col.Set(fmt.Sprintf("KEY_%d", i), []byte("Lorem ipsum dolor sit amet"))
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(time.Since(start).Milliseconds(), "ms")
}
