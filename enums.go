package gokvstore

// CatalogLoadingStrategy The catalog loading strategy
type CatalogLoadingStrategy uint

const (
	// EagerLoad Loads the catalog data when creating the collection with the [NewCollection] method,
	// it is the default strategy and should be used on small collections
	EagerLoad CatalogLoadingStrategy = iota

	// LazyLoad Loads the catalog data only when performing the first read operation of a non indexed
	// key or a count or iteration of the collection entries, its use is recommended for medium to
	// large collections in order to avoid a slow catalog creation with [NewCollection]
	LazyLoad

	// ManualLoad Loads the catalog data only when the [Collection.LoadCatalog] method is manually called,
	// its use is recommended when a fine grained control over the loading is needed, such as when old data
	// is not relevant (eg. fast paths that only new data)
	ManualLoad
)
