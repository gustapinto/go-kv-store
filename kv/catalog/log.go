package catalog

import "time"

// Log Describes a append-only operation on a dataset
type Log struct {
	// Op The operation being applied
	Op Operation `json:"op"`

	// Key The Key-Value entry key
	Key string `json:"key"`

	// Value The Key-Value entry value
	Value []byte `json:"value"`

	// Ts The unix millis epoch timestamp when the operation is being applied
	Ts int64 `json:"ts"`
}

func NewLog(op Operation, key string, value []byte) *Log {
	return &Log{
		Op:    op,
		Key:   key,
		Value: value,
		Ts:    time.Now().UnixMilli(),
	}
}
