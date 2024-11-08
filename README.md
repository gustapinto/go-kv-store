# go-kv-store

A golang embeddable Key-Value store that abstract computation (Collections) from storage (RecordStore)

## Installation

```bash
go get -u github.com/gustapinto/go-kv-store
```

## Example Usage

```go
package main

import (
	"fmt"

	gokvstore "github.com/gustapinto/go-kv-store"
)

func main() {
	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore("./data"))
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

	if err := collection.Delete("1e53ccd4-107f-4491-8b65-bff3e477c201"); err != nil {
		panic(err)
	}
}
```

### Storage options

Today the library has the following storage options
- **Filesystem**, using the `gokvstore.FsRecordStore{...}` store
- **Amazon S3**, using the `gokvstore.S3RecordStore{...}` store
- **In memory**, using the `gokvstore.InMemoryStore{...}` store (this store is not recommended for production usage)

## Concurrency considerations

All library operations are sync and should not be considered safe for concurrent scenarios unless the concurrency details (ex: locking) are handled by the consumer.

## Performance considerations

Note that creating a Collection (via [NewCollection] function or [Collection.NewCollection]) is not free, as it will index every compatible file under the collection root, if you need to save a lot of records is recommended that you use multiple small collections for it and allocated the collections on your application  startup process.

Memory wise this library uses an internal `map[string]string` index, where the map keys are your keys (provided on the Put method) and the values are `uuid v4` fileIds, cacheable will also be indexed in a `map[string][]byte`, with the map values being your values (provided on the Put method).

This library takes an average of 40-45 seconds to Put and 30-40 seconds to index a million files consisting of an `uuid v4` key and a 5 paragraph lorem ipsum body, this test was done using the `gokvstore.FsRecordStore{...}` in a machine with an **Intel Core i5 9400f** CPU, **16gb** of RAM and an entry level **SATA3 SSD**. 
