package gokvstore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Collection Is the base key-value store object, think of it as a Directory or a Folder
// for your records
type Collection struct {
	dataDir       string
	fileName      string
	storeFilePath string
	keyCache      map[string]struct{}
}

var (
	ErrKeyNotFound       = errors.New("key not found in store")
	ErrStoreFileNotFound = errors.New("store file not found")
	recordSeparator      = []byte("<split>")
)

// NewCollection Create a new Store with the specified data directory and file name
func NewCollection(dataDir, fileName string) (*Collection, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, fs.FileMode(0755)); err != nil {
		return nil, err
	}

	store := &Collection{
		dataDir:       absPath,
		fileName:      fileName,
		storeFilePath: filepath.Join(absPath, fileName),
		keyCache:      make(map[string]struct{}),
	}

	if err := store.loadKeyCacheFromStoreFile(); err != nil && !errors.Is(err, ErrStoreFileNotFound) {
		return nil, err
	}

	return store, nil
}

func (*Collection) createRecordBuffer(key string, value []byte) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	if _, err := fmt.Fprintf(&buffer, "%s%s%s", key, recordSeparator, value); err != nil {
		return nil, err
	}

	return &buffer, nil
}

func (*Collection) getKeyAndValueFromBuffer(buffer *bytes.Buffer) (string, []byte, bool) {
	splitRecord := bytes.Split(buffer.Bytes(), recordSeparator)
	if len(splitRecord) != 2 {
		return "", nil, false
	}

	return string(splitRecord[0]), splitRecord[1], true
}

func (l *Collection) openStoreFileScanner(flag int) (*bufio.Scanner, func() error, error) {
	file, err := os.OpenFile(l.storeFilePath, flag, fs.FileMode(0755))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrStoreFileNotFound
		}

		return nil, nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	return scanner, file.Close, nil
}

func (l *Collection) loadKeyCacheFromStoreFile() error {
	scanner, close, err := l.openStoreFileScanner(os.O_RDONLY)
	if err != nil {
		return err
	}
	defer close()

	for scanner.Scan() {
		if key, _, exists := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes())); exists {
			l.keyCache[key] = struct{}{}
		}
	}

	return nil
}

func (l *Collection) insertIntoStoreFile(buffer *bytes.Buffer) error {
	file, err := os.OpenFile(l.storeFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, fs.FileMode(0755))
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, buffer.String()); err != nil {
		return err
	}

	return nil
}

func (l *Collection) updateFromStoreFile(key string, buffer *bytes.Buffer) error {
	tempFile, err := os.CreateTemp(l.dataDir, fmt.Sprintf("*_upd_%s.lock", l.fileName))
	if err != nil {
		return err
	}
	defer tempFile.Close()

	scanner, close, err := l.openStoreFileScanner(os.O_RDWR | os.O_APPEND | os.O_CREATE)
	if err != nil {
		return err
	}
	defer close()

	for scanner.Scan() {
		recordKey, _, exists := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if !exists || recordKey != key {
			if _, err := fmt.Fprintln(tempFile, scanner.Text()); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(tempFile, buffer.String()); err != nil {
				return err
			}
		}
	}

	if err := os.Rename(tempFile.Name(), l.storeFilePath); err != nil {
		return err
	}

	return nil
}

// Collection Creates a new sub Collection, think of it as a subdirectory
func (l *Collection) Collection(dataDir, fileName string) (*Collection, error) {
	partitionPath, err := filepath.Abs(filepath.Join(l.dataDir, dataDir))
	if err != nil {
		return nil, err
	}

	return NewCollection(partitionPath, fileName)
}

// Delete Removes a key from the store, it returns ErrKeyNotFound if the key does not exists into the store
func (l *Collection) Delete(key string) error {
	if _, exists := l.keyCache[key]; !exists {
		return ErrKeyNotFound
	}

	tempFile, err := os.CreateTemp(l.dataDir, fmt.Sprintf("*_del_%s.lock", l.fileName))
	if err != nil {
		return err
	}
	defer tempFile.Close()

	scanner, close, err := l.openStoreFileScanner(os.O_RDWR | os.O_APPEND | os.O_CREATE)
	if err != nil {
		return err
	}
	defer close()

	found := false
	for scanner.Scan() {
		recordKey, _, exists := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if exists && recordKey != key {
			found = true
			continue
		}

		fmt.Fprintln(tempFile, scanner.Text())
	}

	if !found {
		return ErrKeyNotFound
	}

	if err := os.Rename(tempFile.Name(), l.storeFilePath); err != nil {
		return err
	}

	delete(l.keyCache, key)

	return nil
}

// Get Find a value by its key, it returns ErrKeyNotFound if the key does not exists into the store
func (l *Collection) Get(key string) ([]byte, error) {
	scanner, close, err := l.openStoreFileScanner(os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer close()

	for scanner.Scan() {
		recordKey, recordValue, exists := l.getKeyAndValueFromBuffer(bytes.NewBuffer(scanner.Bytes()))
		if !exists || recordKey != key {
			continue
		}

		return recordValue, nil
	}

	return nil, ErrKeyNotFound
}

// Put Insert or delete a key-value record
func (l *Collection) Put(key string, value []byte) error {
	buffer, err := l.createRecordBuffer(key, value)
	if err != nil {
		return err
	}

	if _, exists := l.keyCache[key]; exists {
		if err := l.updateFromStoreFile(key, buffer); err != nil {
			return err
		}

		l.keyCache[key] = struct{}{}
		return nil
	}

	if err := l.insertIntoStoreFile(buffer); err != nil {
		return err
	}

	l.keyCache[key] = struct{}{}
	return nil
}
