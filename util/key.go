package util

import (
	"encoding/binary"
)

func EncodeKey(ts int64, c int64, key []byte) []byte {
	buf := make([]byte, 16+len(key))
	binary.BigEndian.PutUint64(buf[:8], uint64(ts))
	binary.BigEndian.PutUint64(buf[8:16], uint64(c))
	copy(buf[16:], key)
	return buf[:]
}

func DecodeKey(buf []byte) (int64, int64, []byte) {
	ts := int64(binary.BigEndian.Uint64(buf[:8]))
	c := int64(binary.BigEndian.Uint64(buf[8:16]))
	key := buf[16:]
	return ts, c, key
}
