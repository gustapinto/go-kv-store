package catalog

// Log Describes a append-only operation on a dataset
type Log struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key"`
	Value []byte    `json:"value"`
	Ts    int64     `json:"ts"`
}
