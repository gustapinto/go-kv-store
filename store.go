package gokvstore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type operation struct {
	Type  string
	Key   []byte
	Value []byte
}

type Store struct {
	basePath             string
	fileName             string
	storeFilePath        string
	logger               *slog.Logger
	writeCache           sync.Map
	keyCache             sync.Map
	closed               atomic.Bool
	storeIsBeingModified atomic.Bool
}

const (
	readWritePermission = fs.FileMode(0755)

	addOperation    = "PUT-NEW"
	updateOperation = "PUT-UPDATE"
)

var (
	ErrStoreClosed = errors.New("store already closed")
	ErrNotFound    = errors.New("key not found in store")

	separator = []byte("<split>")
)

func NewStore(basePath, fileName string) (*Store, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, readWritePermission); err != nil {
		return nil, err
	}

	logger := slog.Default().With("dir", absPath)
	store := &Store{
		basePath:             absPath,
		fileName:             fileName,
		storeFilePath:        filepath.Join(absPath, fileName),
		logger:               logger,
		writeCache:           sync.Map{},
		keyCache:             sync.Map{},
		closed:               atomic.Bool{},
		storeIsBeingModified: atomic.Bool{},
	}

	if err := store.loadKeyCache(); err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	go store.startWriteThread()

	return store, nil
}

func (*Store) createRecordBuffer(key, value []byte) (*bytes.Buffer, error) {
	line := bytes.Join([][]byte{key, value}, separator)

	return bytes.NewBuffer(line), nil
}

func (*Store) getKeyAndValueFromBuffer(buffer *bytes.Buffer) ([]byte, []byte, bool) {
	splitRecord := bytes.Split(buffer.Bytes(), separator)
	if len(splitRecord) != 2 {
		return nil, nil, false
	}

	return splitRecord[0], splitRecord[1], true
}

func (*Store) makeWriteCacheKey(op operation) string {
	return fmt.Sprintf("%s_%s", op.Type, op.Key)
}

func (l *Store) writeEntryToStoreFile(key, value any) bool {
	l.storeIsBeingModified.Store(true)
	defer func() {
		l.storeIsBeingModified.Store(false)
	}()

	writeCacheKey, ok := key.(string)
	if !ok {
		l.logger.Info("entry removed from write cache, failed to deserialize key", "key", writeCacheKey)
		l.writeCache.Delete(string(writeCacheKey))
		return true
	}

	operation, ok := value.(operation)
	if !ok {
		l.logger.Info("entry removed from write cache, failed to deserialize value", "key", writeCacheKey)
		l.writeCache.Delete(string(writeCacheKey))
		return true
	}

	buffer, err := l.createRecordBuffer(operation.Key, operation.Value)
	if err != nil {
		return true
	}

	if operation.Type == updateOperation {
		tempFile, err := os.CreateTemp(l.basePath, fmt.Sprintf("*_upd_%s.lock", l.fileName))
		if err != nil {
			return true
		}
		defer tempFile.Close()

		scanner, close, err := l.openStoreScanner(os.O_RDWR | os.O_APPEND | os.O_CREATE)
		if err != nil {
			return true
		}
		defer close()

		for scanner.Scan() {
			recordKey, _, ok := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
			if !ok || !bytes.Equal(recordKey, operation.Key) {
				fmt.Fprintln(tempFile, scanner.Text())
			} else {
				l.logger.Info("AAAAA", "key", recordKey, "value", buffer.String())
				fmt.Fprintln(tempFile, buffer.String())
			}
		}

		if err := os.Rename(tempFile.Name(), l.storeFilePath); err != nil {
			return true
		}

		l.logger.Info("entry updated on local storage", "key", key, "value", operation.Value)
	} else {
		file, err := os.OpenFile(l.storeFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, readWritePermission)
		if err != nil {
			return true
		}
		defer file.Close()

		fmt.Fprintln(file, buffer.String())
		l.logger.Info("entry written to local storage", "key", writeCacheKey, "value", operation.Value)
	}

	// Remove already written entry from write cache map
	l.writeCache.Delete(string(writeCacheKey))
	l.keyCache.Store(string(writeCacheKey), struct{}{})
	l.logger.Info("entry removed from write cache, already written to local storage", "key", writeCacheKey)

	return true
}

func (l *Store) startWriteThread() {
	l.logger.Info("starting write thread")

	for {
		if l.storeIsBeingModified.Load() {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		l.writeCache.Range(l.writeEntryToStoreFile)
		if l.closed.Load() {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	l.logger.Info("stoped write thread")
}

func (l *Store) openStoreScanner(flag int) (*bufio.Scanner, func() error, error) {
	file, err := os.OpenFile(l.storeFilePath, flag, readWritePermission)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNotFound
		}

		return nil, nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	return scanner, file.Close, nil
}

func (l *Store) loadKeyCache() error {
	if l.closed.Load() {
		return ErrStoreClosed
	}

	l.logger.Info("loading already written entries from local storage")

	scanner, close, err := l.openStoreScanner(os.O_RDONLY)
	if err != nil {
		return err
	}
	defer close()

	for scanner.Scan() {
		key, value, ok := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if !ok {
			continue
		}

		l.keyCache.Store(string(key), struct{}{})
		l.logger.Info("entry loaded from local storage", "key", key, "value", value)
	}

	return nil
}

func (l *Store) Close() {
	if l.logger != nil {
		l.logger.Debug("closed")
	}

	l.closed.Store(true)
}

func (l *Store) Partition(path string) (*Store, error) {
	return l.PartitionWithFileName(path, l.fileName)
}

func (l *Store) PartitionWithFileName(path, fileName string) (*Store, error) {
	if l.closed.Load() {
		return nil, ErrStoreClosed
	}

	if len(path) == 0 {
		path = generateRandomString(16)
	}

	bucketPath, err := filepath.Abs(filepath.Join(l.basePath, path))
	if err != nil {
		return nil, err
	}

	return NewStore(bucketPath, fileName)
}

func (l *Store) Delete(key []byte) error {
	if l.closed.Load() {
		return ErrStoreClosed
	}

	l.storeIsBeingModified.Store(true)
	defer func() {
		l.storeIsBeingModified.Store(false)
	}()

	tempFile, err := os.CreateTemp(l.basePath, fmt.Sprintf("*_del_%s.lock", l.fileName))
	if err != nil {
		return err
	}
	defer tempFile.Close()

	scanner, close, err := l.openStoreScanner(os.O_RDWR | os.O_APPEND | os.O_CREATE)
	if err != nil {
		return err
	}
	defer close()

	for scanner.Scan() {
		recordKey, _, ok := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if !ok || bytes.Equal(recordKey, key) {
			continue
		}

		fmt.Fprintln(tempFile, scanner.Text())
	}

	if err := os.Rename(tempFile.Name(), l.storeFilePath); err != nil {
		return err
	}

	l.keyCache.Delete(string(key))

	return nil
}

func (l *Store) Get(key []byte) ([]byte, error) {
	if l.closed.Load() {
		return nil, ErrStoreClosed
	}

	if v, ok := l.writeCache.Load(string(key)); ok {
		if value, ok := v.([]byte); ok {
			l.logger.Info("entry retrieved from write cache", "key", string(key))
			return value, nil
		}
	}

	scanner, close, err := l.openStoreScanner(os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer close()

	for scanner.Scan() {
		recordKey, recordValue, ok := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if !ok || !bytes.Equal(recordKey, key) {
			continue
		}

		return recordValue, nil
	}

	return nil, ErrNotFound
}

func (l *Store) Put(key, value []byte) error {
	if l.closed.Load() {
		return ErrStoreClosed
	}

	operationType := addOperation
	if _, ok := l.keyCache.Load(string(key)); ok {
		operationType = updateOperation
	}

	operation := operation{
		Type:  operationType,
		Key:   key,
		Value: value,
	}
	l.writeCache.Store(l.makeWriteCacheKey(operation), operation)
	l.logger.Info("added to write cache", "key", string(key))

	return nil
}
