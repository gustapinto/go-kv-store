package gokvstore

import (
	"github.com/gustapinto/go-kv-store/gen"
)

type recordStore interface {
	// list Lists all record paths in the store
	list() ([]string, error)

	// read Reads a record path into a [gen.Record]
	read(recordPath string) (*gen.Record, error)

	// write Writes a [gen.Record] into the specified path
	write(recordPath string, record *gen.Record) error

	// remove Delete a record
	remove(recordPath string) error

	// makeRecordPath Build a record path from its fileId
	makeRecordPath(fileId string) string

	// makeStoreForCollection Create a new store based on the actual for a sub collection
	makeStoreForCollection(dir string) (recordStore, error)
}
