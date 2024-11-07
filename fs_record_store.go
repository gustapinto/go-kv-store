package gokvstore

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/gustapinto/go-kv-store/gen"
	"google.golang.org/protobuf/proto"
)

// fsRecordStore A filesystem (local disk) based record store
type fsRecordStore struct {
	dataDir string
}

var _ recordStore = (*fsRecordStore)(nil)

// NewFsRecordStore Create a new [fsRecordStore]
func NewFsRecordStore(dataDir string) *fsRecordStore {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil
	}

	if err := os.MkdirAll(absPath, fs.FileMode(0755)); err != nil {
		return nil
	}

	return &fsRecordStore{
		dataDir: absPath,
	}
}

func (f *fsRecordStore) list() (recordPaths []string, err error) {
	builder := strings.Builder{}
	builder.WriteString(f.dataDir)
	builder.WriteString("/*")
	builder.WriteString(protobufBinaryExtension)

	recordsGlobPattern := filepath.Clean(builder.String())
	paths, err := filepath.Glob(recordsGlobPattern)
	if err != nil {
		return nil, err
	}

	return paths, err
}

func (f *fsRecordStore) read(recordPath string) (*gen.Record, error) {
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

func (f *fsRecordStore) remove(recordPath string) error {
	if err := os.Remove(recordPath); err != nil {
		return err
	}

	return nil
}

func (f *fsRecordStore) write(recordPath string, record *gen.Record) error {
	buffer, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(recordPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(buffer); err != nil {
		return err
	}

	return nil
}

func (f *fsRecordStore) makeRecordPath(fileId string) string {
	builder := strings.Builder{}
	builder.WriteString(fileId)
	builder.WriteString(protobufBinaryExtension)

	return filepath.Join(f.dataDir, builder.String())
}

func (f *fsRecordStore) makeStoreForCollection(dir string) (recordStore, error) {
	collectionPath, err := filepath.Abs(filepath.Join(f.dataDir, dir))
	if err != nil {
		return nil, err
	}

	return NewFsRecordStore(collectionPath), nil
}
