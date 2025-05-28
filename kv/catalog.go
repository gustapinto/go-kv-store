package kv

import "github.com/gustapinto/go-kv-store/kv/catalog"

// Catalog The interface implemented by the Key-Value providers, the catalogs are the
// de-facto persistence layer of the Key-Value store
type Catalog interface {
	Log(op catalog.Operation, key string, value []byte) error

	Iter(callback func(log *catalog.Log) (shouldContinue bool)) error
}
