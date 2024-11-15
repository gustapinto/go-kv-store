package gokvstore

import (
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/gustapinto/go-kv-store/gen"
)

// Collection Is the base key-value store object, think of it as a directory or a folder
// for your records
type Collection struct {
	keyToFileIdMapping map[string]string
	cache              map[string][]byte
	store              RecordStore
}

var (
	ErrKeyNotFound  = errors.New("key not found in store")
	ErrInvalidKey   = errors.New("invalid key")
	ErrInvalidValue = errors.New("invalid value")
)

// NewCollection Create a new collection of records.
func NewCollection(store RecordStore) (*Collection, error) {
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

func (*Collection) validateKey(key string) error {
	if len(strings.TrimSpace(key)) == 0 {
		return ErrInvalidKey
	}

	return nil
}

func (*Collection) validateValue(value []byte) error {
	if len(value) == 0 {
		return ErrInvalidValue
	}

	return nil
}

func (c *Collection) loadKeyToFileIdMapping() error {
	paths, err := c.store.List()
	if err != nil {
		return err
	}

	for _, path := range paths {
		record, err := c.store.Read(path)
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

// NewCollection Creates a new sub NewCollection. See [Collection.NewCollection] godoc
// for more details
func (c *Collection) NewCollection(dataDir string) (*Collection, error) {
	store, err := c.store.MakeStoreForCollection(dataDir)
	if err != nil {
		return nil, err
	}

	return NewCollection(store)
}

// Delete Deletes a record from disk, it returns ErrKeyNotFound if the key does not exist in the collection
func (c *Collection) Delete(key string) error {
	if err := c.validateKey(key); err != nil {
		return err
	}

	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return ErrKeyNotFound
	}

	return c.store.Remove(c.store.MakeRecordPath(fileId))
}

// Get Find a value by its key, it returns ErrKeyNotFound if the key does not exist in the collection
func (c *Collection) Get(key string) ([]byte, error) {
	if err := c.validateKey(key); err != nil {
		return nil, err
	}

	if value, exists := c.cache[key]; exists {
		return value, nil
	}

	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	record, err := c.store.Read(c.store.MakeRecordPath(fileId))
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
	if err := c.validateKey(key); err != nil {
		return err
	}

	if err := c.validateValue(value); err != nil {
		return err
	}

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

	if err := c.store.Write(c.store.MakeRecordPath(record.Id), record); err != nil {
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

// Truncate Delete the [Collection] entire data directory
func (c *Collection) Truncate() error {
	return c.store.Truncate()
}
