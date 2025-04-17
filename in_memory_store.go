package gokvstore

import (
	"github.com/gustapinto/go-kv-store/gen"
)

// InMemoryStore A in memory based record store. The use of this store is not
// recommended, as it exists mainly to simplify internal library testing and does
// not support some features (ex: sub collections). Implements [RecordStore]
type InMemoryStore struct {
	data    map[string]*gen.Record
	catalog dataCatalog
}

var _ RecordStore = (*InMemoryStore)(nil)

// NewInMemoryStore Create a new [InMemoryStore]
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		data: make(map[string]*gen.Record),
	}
}

func (i *InMemoryStore) List() ([]string, error) {
	var fileIds []string
	for fileId := range i.data {
		fileIds = append(fileIds, fileId)
	}

	return fileIds, nil
}

func (i *InMemoryStore) MakeRecordPath(fileId string) string {
	return fileId
}

func (i *InMemoryStore) MakeStoreForCollection(_ string) (RecordStore, error) {
	return NewInMemoryStore(), nil
}

func (i *InMemoryStore) Read(recordPath string) (*gen.Record, error) {
	value, exists := i.data[recordPath]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

func (i *InMemoryStore) Remove(recordPath string) error {
	delete(i.data, recordPath)
	return nil
}

func (i *InMemoryStore) Write(recordPath string, record *gen.Record) error {
	i.data[recordPath] = record

	return nil
}

func (i *InMemoryStore) Truncate() error {
	i.data = make(map[string]*gen.Record)
	return nil
}

func (i *InMemoryStore) HasCatalog() bool {
	return len(i.catalog.Entries) > 0
}

func (i *InMemoryStore) ReadCatalog() (*dataCatalog, error) {
	return &i.catalog, nil
}

func (i *InMemoryStore) WriteCatalog(catalog dataCatalog) error {
	i.catalog = catalog
	return nil
}
