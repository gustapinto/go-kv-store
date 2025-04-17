package gokvstore

import (
	"encoding/json"
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

// FsRecordStore A filesystem (local disk) based record store. Implements [RecordStore]
type FsRecordStore struct {
	dataDir string
}

var _ RecordStore = (*FsRecordStore)(nil)

// NewFsRecordStore Create a new [FsRecordStore]
func NewFsRecordStore(dataDir string) *FsRecordStore {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil
	}

	if err := os.MkdirAll(absPath, fs.FileMode(0755)); err != nil {
		return nil
	}

	return &FsRecordStore{
		dataDir: absPath,
	}
}

func (f *FsRecordStore) List() (recordPaths []string, err error) {
	builder := strings.Builder{}
	builder.WriteString(f.dataDir)
	builder.WriteString("/*.binpb")

	recordsGlobPattern := filepath.Clean(builder.String())
	paths, err := filepath.Glob(recordsGlobPattern)
	if err != nil {
		return nil, err
	}

	return paths, err
}

func (f *FsRecordStore) Read(recordPath string) (*gen.Record, error) {
	file, err := os.Open(recordPath)
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

	return &record, nil
}

func (f *FsRecordStore) Remove(recordPath string) error {
	if err := os.Remove(recordPath); err != nil {
		return err
	}

	return nil
}

func (f *FsRecordStore) createRecordFile(recordPath string, buffer []byte) error {
	file, err := os.OpenFile(recordPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buffer)
	return err
}

func (f *FsRecordStore) updateRecordFile(recordPath string, buffer []byte) error {
	tempDir, err := os.MkdirTemp(f.dataDir, "tmp*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	builder := strings.Builder{}
	builder.WriteString(uuid.NewString())
	builder.WriteString(".binpb")

	tempFilePath := filepath.Join(tempDir, builder.String())
	tempFile, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	if _, err := tempFile.Write(buffer); err != nil {
		return err
	}

	return os.Rename(tempFile.Name(), recordPath)
}

func (f *FsRecordStore) Write(recordPath string, record *gen.Record) error {
	buffer, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	if _, err := os.Stat(recordPath); errors.Is(err, os.ErrNotExist) {
		return f.createRecordFile(recordPath, buffer)
	}

	return f.updateRecordFile(recordPath, buffer)
}

func (f *FsRecordStore) MakeRecordPath(fileId string) string {
	builder := strings.Builder{}
	builder.WriteString(fileId)
	builder.WriteString(".binpb")

	return filepath.Join(f.dataDir, builder.String())
}

func (f *FsRecordStore) MakeStoreForCollection(dir string) (RecordStore, error) {
	collectionPath, err := filepath.Abs(filepath.Join(f.dataDir, dir))
	if err != nil {
		return nil, err
	}

	return NewFsRecordStore(collectionPath), nil
}

func (f *FsRecordStore) Truncate() error {
	if err := os.RemoveAll(f.dataDir); err != nil {
		return err
	}

	return nil
}

func (f *FsRecordStore) HasCatalog() bool {
	_, err := os.Stat(f.catalogPath())

	return !os.IsNotExist(err)
}

func (f *FsRecordStore) ReadCatalog() (*dataCatalog, error) {
	catalogBuffer, err := os.ReadFile(f.catalogPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCatalogDoesNotExists
		}

		return nil, err
	}

	var catalog dataCatalog
	if err := json.Unmarshal(catalogBuffer, &catalog); err != nil {
		return nil, err
	}

	return &catalog, nil
}

func (f *FsRecordStore) WriteCatalog(catalog dataCatalog) error {
	catalogBuffer, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return err
	}

	tempPath := f.tempCatalogPath()
	file, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = file.Write(catalogBuffer); err != nil {
		return err
	}

	return os.Rename(tempPath, f.catalogPath())
}

func (f *FsRecordStore) catalogPath() string {
	return filepath.Join(f.dataDir, "_catalog.json")
}

func (f *FsRecordStore) tempCatalogPath() string {
	builder := strings.Builder{}
	builder.WriteString("_catalog_")
	builder.WriteString(uuid.NewString())
	builder.WriteString(".json")

	return filepath.Join(f.dataDir, builder.String())
}
