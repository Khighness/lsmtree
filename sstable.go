package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

// @Author KHighness
// @Update 2023-09-16

const (
	// ssTableMetaFileName is SSTable meta data name, It contains the max SSTable number.
	ssTableMetaFileName = "meta.db"
	// ssTableDataFileName is SSTable data file name. It contains raw data.
	ssTableDataFileName = "data.db"
	// ssTableIndexFileName is SSTable index file name. It contains keys and positions to values in the data file.
	ssTableIndexFileName = "index.db"
	// ssTableSparseIndexFileName is SSTable sparse index file name.
	ssTableSparseIndexFileName = "sparse.db"
	// A flag to open file for new SSTable files: data, index and sparse index.
	newSsTableFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

// createSsTable create a SSTable from the given memTable with the given prefix
// and in the given directory.
func createSsTable(mt *memTable, dbDir string, index, sparseKeyDistance int) error {
	prefix := strconv.Itoa(index) + "-"
	writer, err := newSsTableWriter(dbDir, prefix, sparseKeyDistance)
	if err != nil {
		return fmt.Errorf("failed to create sstable writer: %w", err)
	}

	for it := mt.iterator(); it.hasNext(); {
		key, value := it.next()
		if err := writer.write(key, value); err != nil {
			return fmt.Errorf("failed to create sstable writer: %w", err)
		}
	}

	if err := writer.sync(); err != nil {
		return fmt.Errorf("failed to sync sstable: %w", err)
	}

	if err := writer.close(); err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}

	return nil
}

// searchInSsTable searches a value of the given key in all SSTables, by traversing
// all tables in the directory.
func searchInSsTables(dbDir string, maxIndex int, key []byte) ([]byte, bool, error) {
	for index := maxIndex; index >= 0; index-- {
		value, exists, err := searchInSsTable(dbDir, index, key)
		if err != nil {
			return nil, false, fmt.Errorf("failed to search in sstable with index %d: %w", index, err)
		}

		if exists {
			return value, exists, nil
		}
	}

	return nil, false, nil
}

// searchInSsTable searches a value of the given key in the specific SSTable.
func searchInSsTable(dbDir string, index int, key []byte) ([]byte, bool, error) {
	prefix := strconv.Itoa(index) + "-"

	sparseIndexPath := path.Join(dbDir, prefix+ssTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open sparse index file: %w", err)
	}

	from, to, ok, err := searchInSparseIndex(sparseIndexFile, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in sparse index %s: %w", sparseIndexPath, err)
	}
	if !ok {
		return nil, false, nil
	}

	indexPath := path.Join(dbDir, prefix+ssTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open index file: %w", err)
	}

	offset, ok, err := searchInIndex(indexFile, from, to, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in index file %s: %w", indexPath, err)
	}

	dataPath := path.Join(dbDir, prefix+ssTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open data file: %w", err)
	}

	value, ok, err := searchInDataFile(dataFile, offset, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in data file %s: %w", dataPath, err)
	}

	if err := sparseIndexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close sparse index file: %w", err)
	}

	if err := indexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close index file: %w", err)
	}

	if err := dataFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close data file: %w", err)
	}

	return value, ok, nil
}

// searchInDataFile searches a value by the key in the data file from the given offset,
// The offset must always point to the beginning of the record.
func searchInDataFile(r io.ReadSeeker, offset int, searchKey []byte) ([]byte, bool, error) {
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil {
			if err == io.EOF {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("failed to read: %w", err)
		}

		if bytes.Equal(key, searchKey) {
			return value, true, nil
		}
	}
}

// searchInIndex searches key in the index file in specified range.
func searchInIndex(r io.ReadSeeker, from, to int, searchKey []byte) (int, bool, error) {
	if _, err := r.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil {
			if err == io.EOF {
				return 0, false, nil
			}
			return 0, false, fmt.Errorf("failed to read: %w", err)
		}
		offset := decodeInt(value)

		if bytes.Equal(key, searchKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, fmt.Errorf("failed to seek: %w", err)
			}

			if current >= int64(to) {
				return 0, false, nil
			}
		}
	}
}

// searchInSparseIndex searches a range between which the key is located.
func searchInSparseIndex(r io.Reader, searchKey []byte) (int, int, bool, error) {
	from := -1
	for {
		key, value, err := decode(r)
		if err != nil {
			if err == io.EOF {
				return from, 0, from != -1, nil
			}
			return 0, 0, false, fmt.Errorf("failed to read: %w", err)
		}

		offset := decodeInt(value)
		cmp := bytes.Compare(key, searchKey)
		if cmp == 0 {
			return offset, offset, true, nil
		} else if cmp < 0 {
			from = offset
		} else {
			if from == -1 {
				return 0, 0, false, nil
			} else {
				return from, offset, true, nil
			}
		}
	}
}

// renameSsTable rename SSTable files: data, index and sparse index files.
func renameSsTable(dbDir string, oldPrefix, newPrefix string) error {
	if err := os.Rename(path.Join(dbDir, oldPrefix+ssTableDataFileName), path.Join(dbDir, newPrefix+ssTableDataFileName)); err != nil {
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+ssTableIndexFileName), path.Join(dbDir, newPrefix+ssTableIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename index file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+ssTableSparseIndexFileName), path.Join(dbDir, newPrefix+ssTableSparseIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename sparse index file: %w", err)
	}

	return nil
}

// deleteSsTable deletes SsTable: data, index and sparse index files.
func deleteSsTable(dbDir string, prefixes ...string) error {
	for _, prefix := range prefixes {
		dataPath := path.Join(dbDir, prefix+ssTableDataFileName)
		if err := os.Remove(dataPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", dataPath, err)
		}

		indexPath := path.Join(dbDir, prefix+ssTableIndexFileName)
		if err := os.Remove(indexPath); err != nil {
			return fmt.Errorf("failed to remove index file %s: %w", indexPath, err)
		}

		sparseIndexPath := path.Join(dbDir, prefix+ssTableSparseIndexFileName)
		if err := os.Remove(sparseIndexPath); err != nil {
			return fmt.Errorf("failed to remove sparse index file %s: %w", sparseIndexPath, err)
		}
	}

	return nil
}

// ssTableWriter is a simple abstraction over SSTable, but only for the writing purposes.
type ssTableWriter struct {
	dataFile        *os.File
	indexFile       *os.File
	sparseIndexFile *os.File

	sparseKeyDistance int

	keyNum, dataPos, indexPos int
}

// newSsTableWriter creates a new instance of SSTable writer.
func newSsTableWriter(dbDir, prefix string, sparseKeyDistance int) (*ssTableWriter, error) {
	dataPath := path.Join(dbDir, prefix+ssTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, newSsTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataPath, err)
	}

	indexPath := path.Join(dbDir, prefix+ssTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, newSsTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	sparseIndexPath := path.Join(dbDir, prefix+ssTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, newSsTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open spare index file %s: %w", sparseIndexPath, err)
	}

	return &ssTableWriter{
		dataFile:          dataFile,
		indexFile:         indexFile,
		sparseIndexFile:   sparseIndexFile,
		sparseKeyDistance: sparseKeyDistance,
		keyNum:            0,
		dataPos:           0,
		indexPos:          0,
	}, nil
}

// write writes key and value into the SSTable: data, index and sparse index file.
func (w *ssTableWriter) write(key, value []byte) error {
	dataBytes, err := encode(key, value, w.dataFile)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, w.dataPos, w.indexFile)
	if err != nil {
		return fmt.Errorf("failed to write to the index file: %w", err)
	}

	if w.keyNum%w.sparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, w.indexPos, w.sparseIndexFile); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}
	}

	w.dataPos += dataBytes
	w.indexPos += indexBytes
	w.keyNum++

	return nil
}

// sync commits all written contents to the stable storage.
func (w *ssTableWriter) sync() error {
	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}

	if err := w.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	if err := w.sparseIndexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync sparse index file: %w", err)
	}

	return nil
}

// close closes all associated files with the SSTable.
func (w *ssTableWriter) close() error {
	if err := w.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to cloase data file: %w", err)
	}

	if err := w.indexFile.Close(); err != nil {
		return fmt.Errorf("failed to close index file: %w", err)
	}

	if err := w.sparseIndexFile.Close(); err != nil {
		return fmt.Errorf("failed to close sparse index file: %w", err)
	}

	return nil
}

// updateSsTable updates the current max SSTable number.
func updateSsTableMeta(dbDir string, num, max int) error {
	filePath := path.Join(dbDir, ssTableMetaFileName)
	if err := ioutil.WriteFile(filePath, encodeIntPair(num, max), 0600); err != nil {
		return fmt.Errorf("failed to write %s: %w", filePath, err)
	}

	return nil
}

// readSsTableMeta reads and returns the number of SSTable and the max index.
func readSsTableMeta(dbDir string) (int, int, error) {
	filePath := path.Join(dbDir, ssTableMetaFileName)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, -1, nil
		}
		return 0, 0, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	num, max := decodeIntPair(data)
	return num, max, nil
}
