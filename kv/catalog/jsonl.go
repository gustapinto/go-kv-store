package catalog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// JSONL Implements an append-only log based on the JSON Lines file format
type JSONL struct {
	filename  string
	writeMode WriteMode
}

// NewJSONL Initialize a usable [NewJSONL] catalog
func NewJSONL(filename string, writeMode WriteMode) *JSONL {
	if writeMode > 1 {
		writeMode = Sync
	}

	return &JSONL{
		filename:  filename,
		writeMode: writeMode,
	}
}

// Log Add a new line to the append-only catalog file
func (c *JSONL) Log(op Operation, key string, value []byte) error {
	logFile, err := os.OpenFile(c.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer logFile.Close()

	logJson, err := json.Marshal(Log{
		Op:    op,
		Key:   key,
		Value: value,
		Ts:    time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(logFile, "%s\n", logJson); err != nil {
		return err
	}

	if c.writeMode == Sync {
		return logFile.Sync()
	}

	return nil
}

// Iter Iterate over every entry in the catalog file, applying the callback to every log.
func (c *JSONL) Iter(callback func(log *Log) (shouldContinue bool)) error {
	if callback == nil {
		return nil
	}

	logFile, err := os.Open(c.filename)
	if err != nil {
		return err
	}
	defer logFile.Close()

	scanner := bufio.NewScanner(logFile)
	for scanner.Scan() {
		var log Log
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			continue
		}

		shouldContinue := callback(&log)
		if !shouldContinue {
			break
		}
	}

	return nil
}
