package lsmtree

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

// @Author KHighness
// @Update 2023-09-30

func TestSearchInSsTable(t *testing.T) {
	dbDir, close, err := prepareSsTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		maxIndex int
		key      []byte
		value    []byte
		ok       bool
		hasErr   bool
	}{
		{0, []byte("a"), []byte("va"), true, false},
		{0, []byte("b"), []byte("vb"), true, false},
		{0, []byte("c"), []byte("vc"), true, false},
		{0, []byte("k"), nil, false, false},
		{1, []byte("k"), nil, false, true},
	}

	for _, c := range cases {
		value, ok, err := searchInSsTable(dbDir, c.maxIndex, c.key)
		if c.hasErr && err == nil {
			t.Fatalf("searchInSsTable expected hasErr=true, actual err=nil")
		}
		if !c.hasErr {
			if !((c.value == nil && value == nil) || bytes.Equal(c.value, value)) {
				t.Fatalf("searchInSsTable expected value=%v, actual value=%v", c.value, value)
			}
			if c.ok != ok {
				t.Fatalf("searchInSsTable expected ok=%v, actual ok=%v", c.ok, ok)
			}
		}
	}
}

func prepareMemTable() *memTable {
	mt := newMemTable()

	mt.put([]byte("a"), []byte("va"))
	mt.put([]byte("b"), []byte("vb"))
	mt.put([]byte("c"), []byte("vc"))
	mt.put([]byte("d"), []byte("vd"))
	mt.put([]byte("e"), []byte("ve"))
	mt.put([]byte("f"), []byte("vf"))
	mt.put([]byte("g"), []byte("vg"))

	return mt
}

func prepareSsTable(mt *memTable, index, sparseKeyInstance int) (string, func(), error) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		return "", nil, err
	}

	if err = createSsTable(mt, dbDir, index, sparseKeyInstance); err != nil {
		return "", nil, err
	}

	return dbDir, func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}, nil
}
