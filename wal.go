package lsmtree

import (
	"fmt"
	"io"
	"os"
	"path"
)

// @Author KHighness
// @Update 2023-10-01

// closeWAL closes the current file and open the new file in the truncate mode.
func clearWAL(dbDir string, wal *os.File) (*os.File, error) {
	walPath := path.Join(dbDir, walFileName)
	if err := wal.Close(); err != nil {
		return nil, fmt.Errorf("failed tp close the WAL file %s: %w", walPath, err)
	}

	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open the file %s: %w", walPath, err)
	}

	return wal, nil
}

// appendToWAL appends entry to the WAL file.
func appendToWAL(wal *os.File, key []byte, value []byte) error {
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to the end: %w", err)
	}

	if _, err := encode(key, value, wal); err != nil {
		return fmt.Errorf("failed to encode and write to the file: %w", err)
	}

	if err := wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync the file: %w", err)
	}

	return nil
}

// loadMemTable loads MemTable from the WAL file.
func loadMemTable(wal *os.File) (*memTable, error) {
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the start: %w", err)
	}

	mt := newMemTable()
	for {
		key, value, err := decode(wal)
		if err != nil {
			if err == io.EOF {
				return mt, nil
			} else {
				return nil, fmt.Errorf("failed to read: %w", err)
			}
		}

		if value != nil {
			mt.put(key, value)
		} else {
			mt.delete(key)
		}
	}
}
