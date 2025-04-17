package gokvstore

import (
	"errors"
	"iter"
	"maps"
	"strings"

	"github.com/google/uuid"
	"github.com/gustapinto/go-kv-store/gen"
)

// Collection Is the base key-value store object, think of it as a directory or a folder
// for your records
type Collection struct {
	cache   map[string][]byte
	store   RecordStore
	catalog dataCatalog
}

var (
	ErrKeyNotFound  = errors.New("key not found in store")
	ErrInvalidKey   = errors.New("invalid key")
	ErrInvalidValue = errors.New("invalid value")
)

// NewCollection Create a new collection of records.
func NewCollection(store RecordStore) (*Collection, error) {
	collection := &Collection{
		store: store,
		cache: make(map[string][]byte),
	}

	if hasCatalog := collection.store.HasCatalog(); hasCatalog {
		if err := collection.loadCatalog(); err != nil {
			return nil, err
		}
	} else {
		if err := collection.indexKeysAndFileIds(); err != nil {
			return nil, err
		}

		if err := collection.store.WriteCatalog(collection.catalog); err != nil {
			return nil, err
		}
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

func (c *Collection) indexKeysAndFileIds() error {
	paths, err := c.store.List()
	if err != nil {
		return err
	}

	for _, path := range paths {
		record, err := c.store.Read(path)
		if err != nil {
			return err
		}

		if err := c.addToCatalog(record); err != nil {
			return err
		}

		if record.Cacheable {
			c.cache[record.Key] = record.Value
		}
	}

	return nil
}

// NewCollection Creates a new sub NewCollection. See [NewCollection] godoc
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

	entry, exists := c.catalog.Entries[key]
	if !exists {
		return ErrKeyNotFound
	}

	if err := c.store.Remove(c.store.MakeRecordPath(entry.FileID)); err != nil {
		return err
	}

	delete(c.catalog.Entries, key)

	return c.store.WriteCatalog(c.catalog)
}

// Get Find a value by its key, it returns ErrKeyNotFound if the key does not exist in the collection
func (c *Collection) Get(key string) ([]byte, error) {
	if err := c.validateKey(key); err != nil {
		return nil, err
	}

	if value, exists := c.cache[key]; exists {
		return value, nil
	}

	entry, exists := c.catalog.Entries[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	record, err := c.store.Read(c.store.MakeRecordPath(entry.FileID))
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

// Put Insert or update a record
//
// The "cacheable" parameter controls if the record must also be cached on memory, for
// faster retrieval, updates and inserts are persisted as normal and does not benefits
// from "cacheable", these records will be indexed on [NewCollection] calls
func (c *Collection) Put(key string, value []byte, cacheable bool) error {
	if err := c.validateKey(key); err != nil {
		return err
	}

	if err := c.validateValue(value); err != nil {
		return err
	}

	id := uuid.New().String()
	if entry, exists := c.catalog.Entries[key]; exists {
		id = entry.FileID
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

	if err := c.addToCatalog(record); err != nil {
		return err
	}

	if err := c.store.WriteCatalog(c.catalog); err != nil {
		return err
	}

	if cacheable {
		c.cache[record.Key] = record.Value
	} else {
		delete(c.cache, record.Key)
	}

	return nil
}

// Truncate Clears the [Collection] internal caches and delete their entire data directory
func (c *Collection) Truncate() error {
	clear(c.catalog.Entries)
	clear(c.cache)

	return c.store.Truncate()
}

// Keys Returns an [iter.Seq] for the keys in this collection
func (c *Collection) Keys() iter.Seq[string] {
	return maps.Keys(c.catalog.Entries)
}

// CachedKeys Returns an [iter.Seq] for the cached keys in this collection
func (c *Collection) CachedKeys() iter.Seq[string] {
	return maps.Keys(c.cache)
}

// Exists Checks if a key exists in the [Collection]
func (c *Collection) Exists(key string) bool {
	_, exists := c.catalog.Entries[key]
	return exists
}

func (c *Collection) loadCatalog() error {
	catalog, err := c.store.ReadCatalog()
	if err != nil {
		return err
	}

	if catalog != nil {
		c.catalog = *catalog
	}

	return nil
}

func (c *Collection) addToCatalog(record *gen.Record) error {
	if c.catalog.Entries == nil {
		c.catalog.Entries = make(map[string]dataCatalogEntry)
	}

	c.catalog.Entries[record.Key] = dataCatalogEntry{
		Key:       record.Key,
		FileID:    record.Id,
		Cacheable: record.Cacheable,
	}

	if record.Cacheable {
		c.cache[record.Key] = record.Value
	}

	return nil
}
