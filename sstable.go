package lsmtree

import (
	"io"
	"os"
)

// @Author KHighness
// @Update 2023-09-16

const (
	// ssTableMetaFileName is SSTable meta data name, It contains the max SSTable number.
	ssTableMetaFileName = "mata.db"
	// ssTableDataFileName is SSTable data file name. It contains raw data.
	ssTableDataFileName = "data.db"
	// ssTableIndexFileName is SSTable index file name. It contains keys and positions to values in the data file.
	ssTableIndexFileName = "index.db"
	// ssTableSparseIndexFileName is SSTable sparse index file name.
	ssTableSparseIndexFileName = "sparse.db"
	// A flag to open file for new SSTable files: data, index and sparse index.
	newSsTableFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

// searchInIndex searches key in the index file in specified range.
func searchInIndex(r io.Reader, from, to int, searchKey []byte) (int, bool, error) {

}

// searchInSparseIndex searches a range between which the key is located.
func searchInSparseIndex(r io.Reader, searchKey []byte) (int, int, bool, error) {

}
