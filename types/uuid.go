package types

import (
	"encoding/binary"
	"fmt"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// Uuid represents a universally unique identifier (16 bytes).
//
// Internally stored as a u128 in little-endian byte order (matching
// the BSATN wire format). [Uuid.String] reverses to big-endian and
// returns the standard RFC 4122 hyphenated format
// (e.g., "550e8400-e29b-41d4-a716-446655440000").
//
// Construct with [NewUuid] (from a 16-byte array) or
// [NewUuidFromU128] (from a [Uint128] value).
type Uuid interface {
	bsatn.Serializable
	Bytes() [16]byte
	IsZero() bool
	String() string
}

// NewUuid creates a Uuid from a 16-byte array (big-endian UUID format).
func NewUuid(b [16]byte) Uuid {
	return &uuidImpl{data: b}
}

// NewUuidFromU128 creates a Uuid from a Uint128 value.
func NewUuidFromU128(u Uint128) Uuid {
	return &uuidImpl{data: u.Bytes()}
}

// ReadUuid reads a Uuid from a BSATN reader.
// The wire format is a u128 (16 bytes little-endian).
func ReadUuid(r bsatn.Reader) (Uuid, error) {
	b, err := r.GetBytes(16)
	if err != nil {
		return nil, err
	}
	var data [16]byte
	copy(data[:], b)
	return &uuidImpl{data: data}, nil
}

type uuidImpl struct {
	data [16]byte
}

func (u *uuidImpl) WriteBsatn(w bsatn.Writer) {
	w.PutBytes(u.data[:])
}

func (u *uuidImpl) Bytes() [16]byte { return u.data }

func (u *uuidImpl) IsZero() bool {
	return u.data == [16]byte{}
}

func (u *uuidImpl) String() string {
	// UUID is stored as U128 in LE byte order from BSATN.
	// Reverse to get RFC 4122 (big-endian) byte order for display.
	var be [16]byte
	for i := 0; i < 16; i++ {
		be[i] = u.data[15-i]
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(be[0:4]),
		binary.BigEndian.Uint16(be[4:6]),
		binary.BigEndian.Uint16(be[6:8]),
		binary.BigEndian.Uint16(be[8:10]),
		be[10:16],
	)
}
