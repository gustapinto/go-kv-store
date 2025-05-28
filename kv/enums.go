package kv

// CatalogLoadingStrategy The catalog loading strategy
type CatalogLoadingStrategy uint

const (
	// EagerLoad Loads the catalog data when creating the collection with the [NewCollection] method,
	// it is the default strategy
	EagerLoad CatalogLoadingStrategy = iota

	// LazyLoad Loads the catalog data only when performing the first read operation of a non indexed
	// key or a count or iteration of the collection entries
	LazyLoad

	// ManualLoad Loads the catalog data only when the [Collection.LoadCatalog] method is manually called
	ManualLoad
)
