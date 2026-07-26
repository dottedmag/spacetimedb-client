package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.digitalxero.dev/spacetimedb-client/types"
)

// TestPKKeyMatchesIdentitiesDecodedSeparately is the reason pkKey exists: a
// transaction's delete and insert are decoded independently, so the two
// Identity values are distinct pointers behind the same interface. Keyed by the
// raw value they never match, and an update on any Identity-keyed table (user
// profiles, presence leases) degrades into an unmatched delete + insert.
func TestPKKeyMatchesIdentitiesDecodedSeparately(t *testing.T) {
	var raw [32]byte
	raw[0], raw[31] = 0xc2, 0x3c
	first, second := types.NewIdentity(raw), types.NewIdentity(raw)

	// Map lookup compares interfaces with ==, which for a pointer-backed type is
	// pointer identity — not the 32 bytes. That is the bug pkKey works around
	// (testify's Equal would deep-compare and hide it).
	assert.False(t, any(first) == any(second),
		"two decodes of one identity are distinct map keys")
	assert.True(t, pkKey(first) == pkKey(second), "but they normalize to the same key")

	var other [32]byte
	other[0] = 0xff
	assert.NotEqual(t, pkKey(first), pkKey(types.NewIdentity(other)),
		"different identities stay distinct")

	// The normalized key must be usable as a map key.
	seen := map[any]int{}
	seen[pkKey(first)]++
	seen[pkKey(second)]++
	assert.Equal(t, 2, seen[pkKey(first)], "both rows land on one map entry")
}

// TestPKKeyMatchesConnectionIds covers the SDK's other boxed identifier type,
// which has the same pointer-identity problem.
func TestPKKeyMatchesConnectionIds(t *testing.T) {
	var raw [16]byte
	raw[0] = 0x7f
	assert.Equal(t, pkKey(types.NewConnectionId(raw)), pkKey(types.NewConnectionId(raw)))
}

// TestPKKeyPassesScalarsThrough: scalar primary keys already compare by value
// and must keep doing so (a uint64 PK must not be rewritten into a string that
// could collide with a string PK of the same text).
func TestPKKeyPassesScalarsThrough(t *testing.T) {
	assert.Equal(t, any(uint64(7)), pkKey(uint64(7)))
	assert.Equal(t, any("row-7"), pkKey("row-7"))
	assert.NotEqual(t, pkKey(uint64(7)), pkKey("7"), "a number and its text are different rows")
	assert.Nil(t, pkKey(nil))
}

// TestPKKeyToleratesNonComparableValues: a PK backed by a slice would panic as a
// map key, taking the connection's read loop down. It degrades to a formatted
// key instead.
func TestPKKeyToleratesNonComparableValues(t *testing.T) {
	assert.NotPanics(t, func() {
		key := pkKey([]byte{1, 2, 3})
		seen := map[any]struct{}{}
		seen[key] = struct{}{}
	})
	assert.Equal(t, pkKey([]byte{1, 2, 3}), pkKey([]byte{1, 2, 3}))
	assert.NotEqual(t, pkKey([]byte{1, 2, 3}), pkKey([]byte{9}))
}
