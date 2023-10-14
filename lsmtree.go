package lsmtree

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
)

// @Author KHighness
// @Update 2023-10-01

const (
	// MaxKeySize is the maximum allowed key size.
	MaxKeySize = math.MaxUint16
	// MaxValueSize is the maximum allowed value size.
	MaxValueSize = math.MaxUint16
)

const (
	// walFileName is WAL file name.
	walFileName = "wal.db"
	// defaultMemTableThreshold is default MemTable memory size threshold.
	defaultMemTableThreshold = 64000 // 64KB
	// defaultSparseKeyDistance is default distance between keys in sparse index.
	defaultSparseKeyDistance = 128
	// defaultSsTableNumberThreshold is default SSTable number threshold.
	defaultSsTableNumberThreshold = 10
)

var (
	// ErrKeyRequired represents the key is nil or zero-length.
	ErrKeyRequired = errors.New("key required")
	// ErrKeyRequired represents the value is nil or zero-length.
	ErrValueRequired = errors.New("value required")
	// ErrKeyRequired represents the key size is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")
	// ErrKeyRequired represents the value size is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")
)

// LSM is log-structure merge-tree implementation for storing data in files.
type LSMTree struct {
	// dbDir is the path for directory that stored LSM tree files,
	// it is required to provide dedicated directory for each instance
	// of the tree.
	dbDir string

	// wal is file for storing write-ahead log.
	wal *os.File

	// mt is memory cache of ssTable.
	mt *memTable

	// maxSsTableIndex points to the latest created SSTable on the disk.
	// After MemTable is flushed, thr index is updated.
	// By default -1
	maxSsTableIndex int

	// ssTableNum is current number of flushed and merged SSTables in the durable storage.
	ssTableNum int

	// memTableSizeThreshold is threshold of MemTable's memory size in bytes.
	// If MemTable size in bytes passes the threshold, it must be flushed
	// to the filesystem.
	memTableSizeThreshold int

	// ssTableNumberThreshold is threshold of SSTable's disk size in bytes.
	// If SSTable number passes the threshold, it must be merged to decrease space.
	ssTableNumberThreshold int

	// sparseKeyDistance is distance between keys in sparse index.
	sparseKeyDistance int
}

// MemTableSizeThreshold sets memTableSizeThreshold for LSMTree.
func MemTableSizeThreshold(memTableThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.memTableSizeThreshold = memTableThreshold
	}
}

// SsTableNumberThreshold sets ssTableNumberThreshold for LSMTree.
func SsTableNumberThreshold(ssTableNumberThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.ssTableNumberThreshold = ssTableNumberThreshold
	}
}

// SparseKeyDistance sets sparseKeyDistance for LSMTree.
func SparseKeyDistance(sparseKeyDistance int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

// Open opens the database. Only one instance of the tree is allowed to
// read and write to the directory.
func Open(dbDir string, options ...func(*LSMTree)) (*LSMTree, error) {
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s does not exist", dbDir)
	}

	walPath := path.Join(dbDir, walFileName)
	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", walPath, err)
	}

	mt, err := loadMemTable(wal)
	if err != nil {
		return nil, fmt.Errorf("failed to load memtable from %s: %w", walPath, err)
	}

	ssTableNum, maxSsTableIndex, err := readSsTableMeta(dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read sstable meta: %w", err)
	}

	t := &LSMTree{
		dbDir:                  dbDir,
		wal:                    wal,
		mt:                     mt,
		maxSsTableIndex:        maxSsTableIndex,
		ssTableNum:             ssTableNum,
		memTableSizeThreshold:  defaultMemTableThreshold,
		ssTableNumberThreshold: defaultSsTableNumberThreshold,
		sparseKeyDistance:      defaultSparseKeyDistance,
	}
	for _, option := range options {
		option(t)
	}
	return t, nil
}

// Close closes all allocated resources.
func (t *LSMTree) Close() error {
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", t.wal.Name(), err)
	}

	return nil
}

// Put puts a key-value pair into the db.
func (t *LSMTree) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) == 0 {
		return ErrValueRequired
	} else if uint64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	if err := appendToWAL(t.wal, key, value); err != nil {
		return fmt.Errorf("failed to write wal %s: %w", t.wal.Name(), err)
	}

	if err := t.mt.put(key, value); err != nil {
		return fmt.Errorf("failed to write memtable: %w", err)
	}

	if t.mt.bytes() > t.memTableSizeThreshold {
		if err := t.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	if t.ssTableNum >= t.ssTableNumberThreshold {
		oldestIndex := t.maxSsTableIndex - t.ssTableNum + 1
		if err := mergeSsTables(t.dbDir, oldestIndex, oldestIndex+1, t.sparseKeyDistance); err != nil {
			return fmt.Errorf("failed to merge sstables: %w", err)
		}

		if err := updateSsTableMeta(t.dbDir, t.ssTableNum-1, t.maxSsTableIndex); err != nil {
			return fmt.Errorf("failed to update sstable meta: %w", err)
		}
		t.ssTableNum--
	}

	return nil
}

// Get returns the value according to the key.
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.mt.get(key)
	if exists {
		return value, value != nil, nil
	}

	value, exists, err := searchInSsTable(t.dbDir, t.maxSsTableIndex, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in sstables: %w", err)
	}

	return value, exists, nil
}

// Delete deletes the value by key from the db.
func (t *LSMTree) Delete(key []byte) error {
	if err := appendToWAL(t.wal, key, nil); err != nil {
		return fmt.Errorf("failed to write wal: %w", err)
	}

	if err := t.mt.put(key, nil); err != nil {
		return fmt.Errorf("failed to write memtable: %w", err)
	}

	return nil
}

// flushMemTable flushes current MemTable onto the disk and clear it.
func (t *LSMTree) flushMemTable() error {
	newSsTableNum := t.ssTableNum + 1
	newSsTableIndex := t.maxSsTableIndex + 1

	if err := createSsTable(t.mt, t.dbDir, newSsTableIndex, t.sparseKeyDistance); err != nil {
		return fmt.Errorf("faied to create sstable %d: %w", newSsTableIndex, err)
	}

	if err := updateSsTableMeta(t.dbDir, newSsTableNum, newSsTableIndex); err != nil {
		return fmt.Errorf("failed to update max sstable index %d: %w", newSsTableIndex, err)
	}

	newWal, err := clearWAL(t.dbDir, t.wal)
	if err != nil {
		return fmt.Errorf("failed to clear the WAL file: %w", err)
	}

	t.wal = newWal
	t.mt.clear()
	t.ssTableNum = newSsTableNum
	t.maxSsTableIndex = newSsTableIndex

	return nil
}
