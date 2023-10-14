package lsmtree

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

// @Author KHighness
// @Update 2023-10-02

func TestApi(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	tree, err := Open(
		dbDir,
		SparseKeyDistance(64),
		MemTableSizeThreshold(100),
		SsTableNumberThreshold(3),
	)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	for i := 1; i <= 100; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i * 2)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Put error: %s", err)
		}

		getValue, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get error: %s", err)
		}
		if !ok || !bytes.Equal([]byte(value), getValue) {
			t.Fatalf("Get key: %v, expected value: %v, actual value: %v", key, value, string(getValue))
		}
	}

	for i := 1; i <= 100; i++ {
		if i%2 == 0 {
			key := strconv.Itoa(i)
			err := tree.Delete([]byte(key))
			if err != nil {
				t.Fatalf("Delete error: %s", err)
			}
		}
	}

	for i := 1; i <= 100; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i * 2)
		getValue, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get error: %s", err)
		}

		if i%2 != 0 {
			if !ok || !bytes.Equal([]byte(value), getValue) {
				t.Fatalf("Get key: %v, expected ok: true, expected value: %v, actual ok: %v, actual value: %v",
					key, value, ok, string(getValue))
			}
		} else {
			if ok {
				t.Fatalf("Get key: %v, expected ok: false, expected value: nil, actual ok: %v, actual value: %v",
					key, ok, string(getValue))
			}
		}
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}
}
