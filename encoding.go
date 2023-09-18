package lsmtree

import (
	"encoding/binary"
	"fmt"
	"io"
)

// @Author KHighness
// @Update 2023-09-19

// encode encodes key and value and writes it to the specified writer.
// Returns the number of bytes written and error if occurred.
//	Encode format:
//	[encode total length in bytes][encode key length in bytes][key][value]
// The function must be compatible with decode: encode(decode(v)) == v.
func encode(key []byte, value []byte, w io.Writer) (int, error) {
	bytes := 0

	keyLen := encodeInt(len(key))
	entryLen := len(keyLen) + len(key) + len(value)
	encodedEntryLen := encodeInt(entryLen)

	if n, err := w.Write(encodedEntryLen); err != nil {
		return n, err
	} else {
		bytes += n
	}
	if n, err := w.Write(keyLen); err != nil {
		return n, err
	} else {
		bytes += n
	}
	if n, err := w.Write(key); err != nil {
		return n, err
	} else {
		bytes += n
	}
	if n, err := w.Write(value); err != nil {
		return n, err
	} else {
		bytes += n
	}

	return bytes, nil
}

// decode decodes key and value by reading fro the specified reader.
// Returns the number of the read and error if occurred,
// The function must be compatible with decode: encode(decode(v)) == v.
func decode(r io.Reader) ([]byte, []byte, error) {
	var encodedEntryLen [8]byte
	if _, err := r.Read(encodedEntryLen[:]); err != nil {
		return nil, nil, err
	}

	entryLen := decodeInt(encodedEntryLen[:])
	encodedEntry := make([]byte, entryLen)
	n, err := r.Read(encodedEntry)
	if err != nil {
		return nil, nil, err
	}

	if n < entryLen {
		return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry")
	}

	keyLen := decodeInt(encodedEntry[0:8])
	keyEnd := 8 + keyLen
	key := encodedEntry[8:keyEnd]

	if keyEnd == len(encodedEntry) {
		return key, nil, err
	}

	value := encodedEntry[keyEnd:]
	return key, value, err
}

// encodeInt encodes the int as a slice of bytes.
// The function must be compatible with decodeInt.
func encodeInt(x int) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:]
}

// decodeInt decodes the slice of bytes as an int.
// The function must be compatible with encodeInt.
func decodeInt(buf []byte) int {
	return int(binary.BigEndian.Uint64(buf))
}

// encodeIntPair encodes two ints.
func encodeIntPair(x, y int) []byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(x))
	binary.BigEndian.PutUint64(buf[8:], uint64(y))
	return buf[:]
}

// decodeIntPair decodes two ints.
func decodeIntPair(buf []byte) (int, int) {
	x := int(binary.BigEndian.Uint64(buf[0:8]))
	y := int(binary.BigEndian.Uint64(buf[8:16]))
	return x, y
}
