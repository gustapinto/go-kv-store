package catalog

import "time"

// Log Describes a append-only operation on a dataset
type Log struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key"`
	Value []byte    `json:"value"`
	Ts    int64     `json:"ts"`
}

func NewLog(op Operation, key string, value []byte) *Log {
	return &Log{
		Op:    op,
		Key:   key,
		Value: value,
		Ts:    time.Now().UnixMilli(),
	}
}
