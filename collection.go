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
    your keys (provided on the Put method) and the values are uuid v4 fileIds
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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/gustapinto/go-kv-store/gen"
	"google.golang.org/protobuf/proto"
)

// Collection Is the base key-value store object, think of it as a Directory or a Folder
// for your records
type Collection struct {
	dataDir            string
	keyToFileIdMapping map[string]string
}

var ErrKeyNotFound = errors.New("key not found in store")

const protobufBinaryExtension = ".binpb"

// NewCollection Create a new collection of records.
func NewCollection(dataDir string) (*Collection, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, fs.FileMode(0755)); err != nil {
		return nil, err
	}

	store := &Collection{
		dataDir:            absPath,
		keyToFileIdMapping: make(map[string]string),
	}

	if err := store.loadKeyToFileIdMapping(store.dataDir); err != nil {

		return nil, err
	}

	return store, nil
}

func (c *Collection) getFilePathFromFileId(fileId string) string {
	builder := strings.Builder{}
	builder.WriteString(fileId)
	builder.WriteString(protobufBinaryExtension)

	return filepath.Join(c.dataDir, builder.String())
}

func (c *Collection) loadKeyToFileIdMapping(dataDir string) error {
	builder := strings.Builder{}
	builder.WriteString(dataDir)
	builder.WriteString("/*")
	builder.WriteString(protobufBinaryExtension)

	paths, err := filepath.Glob(filepath.Clean(builder.String()))
	if err != nil {
		return err
	}

	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		buffer, err := io.ReadAll(file)
		if err != nil {
			return err
		}

		var record gen.Record
		if err := proto.Unmarshal(buffer, &record); err != nil {
			return err
		}

		c.keyToFileIdMapping[record.Key] = record.Id
	}

	return nil
}

// Collection Creates a new sub Collection. See [Collection.NewCollection] godoc
// for more details
func (c *Collection) Collection(dataDir, fileName string) (*Collection, error) {
	partitionPath, err := filepath.Abs(filepath.Join(c.dataDir, dataDir))
	if err != nil {
		return nil, err
	}

	return NewCollection(partitionPath)
}

// Delete Deletes a record from disk, it returns ErrKeyNotFound if the key does not exists in the collection
func (c *Collection) Delete(key string) error {
	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return ErrKeyNotFound
	}

	return os.Remove(c.getFilePathFromFileId(fileId))
}

// Get Find a value by its key, it returns ErrKeyNotFound if the key does not exists in the collection
func (c *Collection) Get(key string) ([]byte, error) {
	fileId, exists := c.keyToFileIdMapping[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	file, err := os.OpenFile(c.getFilePathFromFileId(fileId), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buffer, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var record gen.Record
	if err := proto.Unmarshal(buffer, &record); err != nil {
		return nil, err
	}

	return record.Value, nil
}

// Put Insert or update (Upsert) a record
func (c *Collection) Put(key string, value []byte) error {
	id := uuid.New().String()
	if fileId, exists := c.keyToFileIdMapping[key]; exists {
		id = fileId
	}

	record := &gen.Record{
		Id:    id,
		Key:   key,
		Value: value,
	}

	buffer, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(c.getFilePathFromFileId(record.Id), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(buffer); err != nil {
		return err
	}

	c.keyToFileIdMapping[key] = record.Id
	return nil
}
