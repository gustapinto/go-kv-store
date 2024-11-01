package gokvstore

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LocalStorageStore struct {
	basePath      string
	storeFilePath string
	logger        *slog.Logger
	writeCache    sync.Map
	closed        atomic.Bool
}

const (
	ReadWritePermission = fs.FileMode(0755)
)

var (
	ErrStoreClosed = errors.New("store already closed")
	ErrNotFound    = errors.New("key not found in store")
)

func NewLocalStorageStore(basePath string) (*LocalStorageStore, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, ReadWritePermission); err != nil {
		return nil, err
	}

	logger := slog.Default().With("dir", absPath)
	store := &LocalStorageStore{
		basePath:      absPath,
		storeFilePath: filepath.Join(absPath, "store_data.kv"),
		logger:        logger,
		writeCache:    sync.Map{},
		closed:        atomic.Bool{},
	}

	go store.startWriteThread()

	return store, nil
}

func (l *LocalStorageStore) writeEntryToLocalStorage(k, v any) bool {
	key, okKey := k.(string)
	value, okValue := v.(string)

	if !okKey || !okValue {
		l.logger.Info("entry removed from write cache, failed to convert to string", "key", key)
		l.writeCache.Delete(key)
		return true
	}

	file, err := os.OpenFile(l.storeFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, ReadWritePermission)
	if err != nil {
		return true
	}
	defer file.Close()

	line := fmt.Sprintf(`%v,%v`, key, value)
	fmt.Fprintln(file, line)

	l.logger.Info("entry written to local storage", "key", key, "value", value)

	// Remove already written entry from write cache map
	l.writeCache.Delete(key)
	l.logger.Info("entry removed from write cache, already written to local storage", "key", key)

	return true
}

func (l *LocalStorageStore) startWriteThread() {
	l.logger.Info("starting write thread")

	for {
		l.writeCache.Range(l.writeEntryToLocalStorage)
		if l.closed.Load() {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	l.logger.Info("stoped write thread")
}

func (l *LocalStorageStore) Close() {
	l.logger.Debug("closed")
	l.closed.Swap(true)
}

func (l *LocalStorageStore) Partition(path string) (*LocalStorageStore, error) {
	if l.closed.Load() {
		return nil, ErrStoreClosed
	}

	if len(path) == 0 {
		path = RandomString(16)
	}

	bucketPath, err := filepath.Abs(filepath.Join(l.basePath, path))
	if err != nil {
		return nil, err
	}

	return NewLocalStorageStore(bucketPath)
}

func (l *LocalStorageStore) DEL(key string) error {
	if l.closed.Load() {
		return ErrStoreClosed
	}

	tempFile, err := os.CreateTemp("", "store_data_*.kv")
	if err != nil {
		return err
	}

	file, err := os.OpenFile(l.storeFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, ReadWritePermission)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ",")

		if line[0] == key {
			continue
		}

		fmt.Fprintln(tempFile, scanner.Text())
	}

	return os.Rename(tempFile.Name(), l.storeFilePath)
}

func (l *LocalStorageStore) GET(key string) (string, error) {
	if l.closed.Load() {
		return "", ErrStoreClosed
	}

	if v, ok := l.writeCache.Load(key); ok {
		if value, ok := v.(string); ok {
			l.logger.Info("entry retrieved from write cache", "key", string(key))
			return value, nil
		}
	}

	file, err := os.OpenFile(l.storeFilePath, os.O_RDONLY, ReadWritePermission)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}

		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ",")
		if line[0] == key {
			l.logger.Info("entry retrieved from local storage", "key", string(key))
			return line[1], err
		}
	}

	return "", nil
}

func (l *LocalStorageStore) PUT(key string, value string) error {
	if l.closed.Load() {
		return ErrStoreClosed
	}

	if _, ok := l.writeCache.Load(key); ok {
		return nil
	}

	l.writeCache.Store(key, value)
	l.logger.Info("added to write cache", "key", string(key))

	return nil
}
