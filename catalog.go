package gokvstore

import "errors"

var (
	ErrCatalogDoesNotExists = errors.New("catalog does not exists for this collection")
)

type dataCatalogEntry struct {
	Key       string `json:"key"`
	FileID    string `json:"filePath"`
	Cacheable bool   `json:"cacheable"`
}

type dataCatalog struct {
	Entries map[string]dataCatalogEntry `json:"entries"`
}
