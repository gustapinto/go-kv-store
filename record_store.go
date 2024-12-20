package gokvstore

import (
	"github.com/gustapinto/go-kv-store/gen"
)

type RecordStore interface {
	// List Lists all record paths in the store
	List() ([]string, error)

	// Read Reads a record path into a [gen.Record]
	Read(recordPath string) (*gen.Record, error)

	// Write Writes a [gen.Record] into the specified path
	Write(recordPath string, record *gen.Record) error

	// Remove Delete a record
	Remove(recordPath string) error

	// MakeRecordPath Build a record path from its fileId
	MakeRecordPath(fileId string) string

	// MakeStoreForCollection Create a new store based on the actual for a sub collection
	MakeStoreForCollection(dir string) (RecordStore, error)

	// Truncate Deletes the store data directory
	Truncate() error
}
