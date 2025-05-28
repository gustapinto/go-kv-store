package catalog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Local Implements an append-only-log catalog based on the JSON Lines file format
type Local struct {
	filename  string
	writeMode WriteMode
}

// NewLocal Initialize a usable [NewLocal] catalog
func NewLocal(filename string, writeMode WriteMode) *Local {
	if writeMode > 1 {
		writeMode = Sync
	}

	return &Local{
		filename:  filename,
		writeMode: writeMode,
	}
}

// Append Add a new line to the append-only catalog file
func (c *Local) Append(log *Log) error {
	if log == nil {
		return errors.New("log cannot be nil")
	}

	logFile, err := os.OpenFile(c.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer logFile.Close()

	logJson, err := json.Marshal(log)
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

// Iter Iterate over every entry in the catalog file, applying the callback to every log
func (c *Local) Iter(callback func(log *Log) (shouldContinue bool)) error {
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
