package gokvstore

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gustapinto/go-kv-store/gen"
	"google.golang.org/protobuf/proto"
)

// Collection Is the base key-value store object, think of it as a Directory or a Folder
// for your records
type Collection struct {
	dataDir            string
	keyToFileIdMapping map[string]string
	mu                 sync.Mutex
}

var ErrKeyNotFound = errors.New("key not found in store")

// NewCollection Create a new Store with the specified data directory and file name
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
	builder.WriteString(".binpb")

	return filepath.Join(c.dataDir, builder.String())
}

func (c *Collection) loadKeyToFileIdMapping(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		file, err := os.Open(filepath.Join(root, entry.Name()))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		buffer, err := io.ReadAll(file)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		var record gen.Record
		if err := proto.Unmarshal(buffer, &record); err != nil {
			fmt.Println(err.Error())
			continue
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		c.keyToFileIdMapping[record.Key] = record.Id
	}

	return nil
}

// Collection Creates a new sub Collection, think of it as a subdirectory
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
