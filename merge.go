package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

// @Author KHighness
// @Update 2023-10-01

// mergeSsTables merges SSTables with index a and b
// and creates new merge table with index b.
// The index a must be less than be and to be older.
func mergeSsTables(dbDir string, a, b int, sparseKeyDistance int) error {
	mergePrefix := "merge"
	aPrefix := strconv.Itoa(a) + "-"
	bPrefix := strconv.Itoa(b) + "-"

	aPath := path.Join(dbDir, aPrefix+ssTableDataFileName)
	aIt, err := newDataFileIterator(aPath)
	if err != nil {
		return fmt.Errorf("failed to instantiate for %s: %w", aPath, err)
	}

	bPath := path.Join(dbDir, bPrefix+ssTableDataFileName)
	bIt, err := newDataFileIterator(bPath)
	if err != nil {
		return fmt.Errorf("failed to iterator for %s: %w", bPath, err)
	}

	writer, err := newSsTableWriter(dbDir, mergePrefix, sparseKeyDistance)
	if err != nil {
		return fmt.Errorf("failed to instantiate sstable writer: %w", err)
	}

	if err := merge(aIt, bIt, writer); err != nil {
		return fmt.Errorf("failed tomerge sstable: %w", err)
	}

	if err := aIt.close(); err != nil {
		return fmt.Errorf("failed to close iterator for %s: %w", aPath, err)
	}

	if err := bIt.close(); err != nil {
		return fmt.Errorf("failed to close iterator for %s: %w", bPath, err)
	}

	if err := writer.close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if err := deleteSsTable(dbDir, aPrefix, bPrefix); err != nil {
		return fmt.Errorf("failed to delete sstable: %w", err)
	}

	if err := renameSsTable(dbDir, mergePrefix, bPrefix); err != nil {
		return fmt.Errorf("failed to rename sstable: %w", err)
	}

	return nil
}

// merge merges keys and values from a and b iterators and writs them
// into the SSTable using SStable writer.
func merge(aIt, bIt *dataFileIterator, writer *ssTableWriter) error {
	var aKey, aValue, bKey, bValue []byte
	for {
		if aKey == nil && aIt.hasNext() {
			if k, v, err := aIt.next(); err != nil {
				return fmt.Errorf("failed to get next for a: %w", err)
			} else {
				aKey, aValue = k, v
			}
		}

		if bKey == nil && bIt.hasNext() {
			if k, v, err := bIt.next(); err != nil {
				return fmt.Errorf("failed to get next for b: %w", err)
			} else {
				bKey, bValue = k, v
			}
		}

		if aKey == nil && bKey == nil && !aIt.hasNext() && !bIt.hasNext() {
			return nil
		}

		if aKey != nil && bKey != nil {
			cmp := bytes.Compare(aKey, bKey)
			if cmp == 0 {
				// aKey == bKey, ignore aKey since bKey is newer.
				if err := writer.write(bKey, bValue); err != nil {
					return fmt.Errorf("failed to write: %w", err)
				}
				aKey, aValue, bKey, bValue = nil, nil, nil, nil
			} else if cmp > 0 {
				if err := writer.write(bKey, bValue); err != nil {
					return fmt.Errorf("failed to write: %w", err)
				}
				bKey, bValue = nil, nil
			} else if cmp < 0 {
				if err := writer.write(aKey, aValue); err != nil {
					return fmt.Errorf("failed to write: %w", err)
				}
				aKey, aValue = nil, nil
			}
		} else if aKey != nil {
			if err := writer.write(aKey, aValue); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
			aKey, aValue = nil, nil
		} else {
			if err := writer.write(bKey, bValue); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
			bKey, bValue = nil, nil
		}
	}
}

// dataFileIterator allows simple iteration over the data file.
type dataFileIterator struct {
	dataFile *os.File
	key      []byte
	value    []byte
	end      bool
	closed   bool
}

// newDataFileIterator instantiates new data file iterator.
func newDataFileIterator(path string) (*dataFileIterator, error) {
	dataFile, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", path, err)
	}

	key, value, err := decode(dataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}

	end := err == io.EOF
	return &dataFileIterator{
		dataFile: dataFile,
		key:      key,
		value:    value,
		end:      end,
		closed:   false,
	}, nil
}

// hasNext returns true if there is next element.
func (it *dataFileIterator) hasNext() bool {
	return !it.end
}

// next returns the current key and value and advances the iterator position.
func (it *dataFileIterator) next() ([]byte, []byte, error) {
	key, value := it.key, it.value

	nextKey, nextValue, err := decode(it.dataFile)
	if err != nil {
		if err == io.EOF {
			it.end = true
		} else {
			return nil, nil, fmt.Errorf("failed to read: %w", err)
		}
	}

	it.key = nextKey
	it.value = nextValue

	return key, value, nil
}

// close closes associated file.
func (it *dataFileIterator) close() error {
	if it.closed {
		return nil
	}

	if err := it.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close: %w", err)
	}

	it.closed = true
	return nil
}
