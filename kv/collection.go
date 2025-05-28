package kv

import (
	"errors"
	"os"
	"sync"

	"github.com/gustapinto/go-kv-store/kv/catalog"
)

// Collection A Key-Value data collection
type Collection struct {
	mu                     sync.Mutex
	catalog                Catalog
	catalogLoadingStrategy CatalogLoadingStrategy
	catalogLoaded          bool
	state                  map[string][]byte
}

// NewCollection Initializes a working [Collection]. It will also load the catalog with [Collection.LoadCatalog] if
// the collection loadingStrategy is equal to [EagerLoad]
func NewCollection(catalog Catalog, loadingStrategy CatalogLoadingStrategy) (*Collection, error) {
	if catalog == nil {
		return nil, errors.New("the catalog must not be nil")
	}

	if loadingStrategy > 3 {
		return nil, errors.New("invalid CatalogLoadingStrategy")
	}

	collection := Collection{
		mu:                     sync.Mutex{},
		catalog:                catalog,
		catalogLoadingStrategy: loadingStrategy,
		catalogLoaded:          false,
		state:                  map[string][]byte{},
	}

	if collection.catalogLoadingStrategy == EagerLoad {
		if err := collection.LoadCatalog(); err != nil {
			return nil, err
		}
	}

	return &collection, nil
}

// LoadCatalog Indexes the collection persisted catalog in memory, it is necessary to acess Key-Value
// pairs that only exists on disk, but the loading can take some time, depending on the collection size
func (c *Collection) LoadCatalog() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.catalog.Iter(func(log *catalog.Log) bool {
		if log == nil {
			return true
		}

		switch log.Op {
		case catalog.Set:
			c.state[log.Key] = log.Value
		case catalog.Del:
			delete(c.state, log.Key)
		}

		return true
	})
	if err != nil {
		if os.IsNotExist(err) {
			c.catalogLoaded = true
			return nil
		}

		return err
	}

	c.catalogLoaded = true

	return nil
}

// Set Add a Key-Value entry to the collection
func (c *Collection) Set(key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.catalog.Log(catalog.Set, key, value); err != nil {
		return err
	}

	c.state[key] = value

	return nil
}

// Del Removes a Key-Value entry from the collection
func (c *Collection) Del(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.state[key]; !exists {
		return nil
	}

	if err := c.catalog.Log(catalog.Del, key, nil); err != nil {
		return err
	}

	delete(c.state, key)

	return nil
}

// Get Find a Key-Value entry from the collection. It will also load the catalog with [Collection.LoadCatalog] if
// the collection catalogLoadingStrategy is equal to [LazyLoad]
func (c *Collection) Get(key string) ([]byte, bool) {
	value, exists := c.state[key]
	if exists {
		return value, true
	}

	if !c.catalogLoaded && c.catalogLoadingStrategy == LazyLoad {
		if err := c.LoadCatalog(); err != nil {
			return nil, false
		}
	}

	value, exists = c.state[key]
	if !exists {
		return nil, false
	}

	return value, true
}

// Len Return the collection size. It will also load the catalog with [Collection.LoadCatalog] if
// the collection catalogLoadingStrategy is equal to [LazyLoad]
func (c *Collection) Len() int64 {
	if !c.catalogLoaded && c.catalogLoadingStrategy == LazyLoad {
		if err := c.LoadCatalog(); err != nil {
			return 0
		}
	}

	return int64(len(c.state))
}

// Iter Over all elements in the collection, applying the callback to every element
func (c *Collection) Iter(callback func(key string, value []byte) (shouldContinue bool)) {
	for key, value := range c.state {
		shouldContinue := callback(key, value)
		if !shouldContinue {
			break
		}
	}
}
