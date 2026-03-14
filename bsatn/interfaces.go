package bsatn

// Writer is a streaming binary encoder for the BSATN format.
//
// Writer accumulates encoded data in an auto-growing internal buffer.
// All multi-byte integers are written in little-endian byte order.
// Strings are length-prefixed with a u32 LE count followed by UTF-8 bytes.
//
// Create a Writer with [NewWriter], write values using the Put* methods,
// then retrieve the accumulated bytes with Bytes.
type Writer interface {
	PutBool(v bool)
	PutU8(v uint8)
	PutU16(v uint16)
	PutU32(v uint32)
	PutU64(v uint64)
	PutI8(v int8)
	PutI16(v int16)
	PutI32(v int32)
	PutI64(v int64)
	PutF32(v float32)
	PutF64(v float64)
	PutString(v string)
	PutBytes(v []byte)    // raw bytes, no length prefix
	PutArrayLen(n uint32) // write u32 LE length prefix for arrays
	PutMapLen(n uint32)   // write u32 LE length prefix for maps
	PutSumTag(tag uint8)  // write u8 variant tag for sum types
	Bytes() []byte        // return the accumulated buffer
	Reset()               // clear buffer, keep capacity
	Len() int             // current accumulated length
}

// Reader is a streaming binary decoder for the BSATN format.
//
// Reader maintains a position cursor that advances as values are read.
// All multi-byte integers are read in little-endian byte order.
// If there are insufficient bytes remaining for a read operation,
// an [ErrBufferTooShort] error is returned.
//
// Create a Reader with [NewReader] or [NewZeroCopyReader].
type Reader interface {
	GetBool() (bool, error)
	GetU8() (uint8, error)
	GetU16() (uint16, error)
	GetU32() (uint32, error)
	GetU64() (uint64, error)
	GetI8() (int8, error)
	GetI16() (int16, error)
	GetI32() (int32, error)
	GetI64() (int64, error)
	GetF32() (float32, error)
	GetF64() (float64, error)
	GetString() (string, error)
	GetBytes(n int) ([]byte, error) // read exactly n raw bytes
	GetArrayLen() (uint32, error)
	GetMapLen() (uint32, error)
	GetSumTag() (uint8, error)
	Remaining() int
}

// Serializable is the key interface for types that can encode themselves
// as BSATN binary data. Implement WriteBsatn to write each field
// sequentially to the [Writer] using the appropriate Put* method.
//
// All SpacetimeDB domain types ([types.Identity], [types.Timestamp], etc.)
// implement Serializable. User-defined table structs should also implement
// this interface to enable encoding reducer arguments and cache keys.
type Serializable interface {
	WriteBsatn(w Writer)
}
