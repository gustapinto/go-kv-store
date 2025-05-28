package kv

import "github.com/gustapinto/go-kv-store/kv/catalog"

// Catalog The interface implemented by the Key-Value providers, the catalogs are the
// de-facto persistence layer of the Key-Value store
type Catalog interface {
	// Append Inserts a new log into the catalog
	Append(log *catalog.Log) error

	// Iter Iterate over all logs in the catalog, applying the callback function
	Iter(callback func(log *catalog.Log) (shouldContinue bool)) error
}
