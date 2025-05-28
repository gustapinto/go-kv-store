package catalog

// WriteMode The catalog write mode
type WriteMode uint

const (
	// Sync Writes files synchronously, calling [File.Sync], its is slower, but have more consistency
	// of a succesful write, it is the default write mode
	Sync WriteMode = iota

	// BufferedWrites files asynchronously, using the operatig system default buffer, it is faster
	// but less consistent than [Sync]
	Buffered
)

// Operation The Log operation type
type Operation string

const (
	// Set The upsert operation, it indicates a write or overwrite of a Key-Value pair
	Set Operation = "set"

	// Delete The delete operation, it indicates a removal of a Key-Value pair
	Del Operation = "del"
)
