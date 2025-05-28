package kv

import "github.com/gustapinto/go-kv-store/kv/catalog"

// Catalog The interface implemented by the Key-Value low level store providers
type Catalog interface {
	Log(op catalog.Operation, key string, value []byte) error

	Iter(callback func(log *catalog.Log) (shouldContinue bool)) error
}
