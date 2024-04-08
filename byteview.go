package gocache

import "time"

// A ByteView holds an immutable view of bytes.  这是一个只读的数据结构
type ByteView struct {
	b []byte
	e time.Time
}

// Len returns the view's length
func (v ByteView) Len() int {
	return len(v.b)
}

func (v ByteView) Expire() time.Time {
	return v.e
}

// ByteSlice returns a copy of the data as a byte slice.
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

// String returns the data as a string, making a copy if necessary.
func (v ByteView) String() string {
	return string(v.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
