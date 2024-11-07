/*
A golang embeddable key-value store based on protobuf files. All operations are
sync and should not be considered safe for concurrent scenarios unless the
concurrency details (locking, etc...) is handled by the consumer.

This library works with collections of key-value records, with each collection
representing a directory on your file system and each ke-value records a binary
protobuf (.binpb) file

Note that creating a Collection object with the [NewCollection] or [the collection.Collection]
methods is not free, as it will index every compatible file under the dataDir
directory, if you need to save a lot of records is recommended that you use
multiple partitions for it and allocated the collections on your application
startup process

Performance considerations:
  - Memory wise this lirary will create a map[string]string index, where the keys are
    your keys (provided on the Put method) and the values are uuid v4 fileIds.
  - When using cacheable records this library will also create a map[string][]byte,
    with the keys and values stored in memory
  - Takes a average of 40-45 seconds to put a million files consisting of a uuid v4 key
    and a 5 paragraph lorem ipsum body
  - It takes a average of 30-40  seconds to index a million files consisting of a uuid v4 key
    and a 5 paragraph lorem ipsum body

Machine used for performance testing:
  - Core i5 94000f
  - 16gb ram
  - entry-level 120gb SATA3 SSD
*/
package gokvstore

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/gustapinto/go-kv-store/gen"
)

// Collection Is the base key-value store object, think of it as a Directory or a Folder
// for your records
type Collection struct {
	keyToFileIdMapping map[string]string
	cache              map[string][]byte
	store              recordStore
}

var ErrKeyNotFound = errors.New("key not found in store")

const protobufBinaryExtension = ".binpb"

// NewCollection Create a new collection of records.
//
// The store parameter is the engine used by the collection to persist data
func NewCollection(store recordStore) (*Collection, error) {
	collection := &Collection{
		store:              store,
		keyToFileIdMapping: make(map[string]string),
		cache:              make(map[string][]byte),
	}

	if err := collection.loadKeyToFileIdMapping(); err != nil {
		return nil, err
	}

	return collection, nil
}

func (c *Collection) loadKeyToFileIdMapping() error {
	paths, err := c.store.list()
	if err != nil {
		return err
	}

	for _, path := range paths {
		record, err := c.store.read(path)
		if err != nil {
			return err
		}

		c.keyToFileIdMapping[record.Key] = record.Id

		if record.Cacheable {
			c.cache[record.Key] = record.Value
		}
	}

	return nil
}

// Collection Creates a new sub Collection. See [Collection.NewCollection] godoc
// for more details
func (c *Collection) Collection(dataDir string) (*Collection, error) {
	store, err := c.store.makeStoreForCollection(dataDir)
	if err != nil {
		return nil, err
	}

	return NewCollection(store)
}

// Delete Deletes a record from disk, it returns ErrKeyNotFound if the key does not exists in the collection
func (c *Collection) Delete(key string) error {
	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return ErrKeyNotFound
	}

	return c.store.remove(c.store.makeRecordPath(fileId))
}

// Get Find a value by its key, it returns ErrKeyNotFound if the key does not exists in the collection
func (c *Collection) Get(key string) ([]byte, error) {
	if value, exists := c.cache[key]; exists {
		fmt.Println("retrieved from cache")
		return value, nil
	}

	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	record, err := c.store.read(c.store.makeRecordPath(fileId))
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

// Put Insert or update (Upsert) a record
//
// The "cacheable" parameter controls if the record must also be cached on memory, for
// faster retrieval, updates and inserts are persisted as normal
//
// Cacheable records will be indexed on Collection creation
func (c *Collection) Put(key string, value []byte, cacheable bool) error {
	id := uuid.New().String()
	if fileId, exists := c.keyToFileIdMapping[key]; exists {
		id = fileId
	}

	record := &gen.Record{
		Id:        id,
		Key:       key,
		Value:     value,
		Cacheable: cacheable,
	}

	if err := c.store.write(c.store.makeRecordPath(record.Id), record); err != nil {
		return err
	}

	c.keyToFileIdMapping[record.Key] = record.Id

	if cacheable {
		c.cache[record.Key] = record.Value
	} else {
		delete(c.cache, record.Key)
	}

	return nil
}
