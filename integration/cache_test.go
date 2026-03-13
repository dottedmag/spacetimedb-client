//go:build integration

package integration_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/integration/schema"
)

func TestCache_InsertCallback(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)

	inserted := make(chan any, 1)
	tc.OnInsert(func(row any) {
		select {
		case inserted <- row:
		default:
		}
	})

	err := conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(42))
	require.NoError(t, err)

	select {
	case row := <-inserted:
		require.NotNil(t, row)
		typed, ok := row.(*schema.Row[uint32])
		require.True(t, ok, "inserted row should be *Row[uint32]")
		assert.Equal(t, uint32(42), typed.Value)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for insert callback")
	}
}

func TestCache_DeleteCallback(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.UniqueU8Def)
	subscribe(t, conn, "SELECT * FROM unique_u_8")

	tc := conn.Cache().GetTable("unique_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(245)

	// Insert a row first.
	err := conn.CallReducer("insert_unique_u_8", schema.InsertUniqueU8Args(testKey, 999))
	require.NoError(t, err)
	waitForCount(t, tc, baseline+1, defaultTimeout)

	deleted := make(chan any, 1)
	tc.OnDelete(func(row any) {
		select {
		case deleted <- row:
		default:
		}
	})

	// Delete the row.
	err = conn.CallReducer("delete_unique_u_8", schema.DeleteUniqueU8Args(testKey))
	require.NoError(t, err)

	select {
	case row := <-deleted:
		require.NotNil(t, row)
		typed, ok := row.(*schema.KeyDataRow[uint8])
		require.True(t, ok, "deleted row should be *KeyDataRow[uint8]")
		assert.Equal(t, testKey, typed.Key)
		assert.Equal(t, int32(999), typed.Data)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for delete callback")
	}
}

func TestCache_UpdateCallback(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.UniqueU8Def)
	subscribe(t, conn, "SELECT * FROM unique_u_8")

	tc := conn.Cache().GetTable("unique_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(246)

	// Insert a row.
	err := conn.CallReducer("insert_unique_u_8", schema.InsertUniqueU8Args(testKey, 111))
	require.NoError(t, err)
	waitForCount(t, tc, baseline+1, defaultTimeout)

	type updatePair struct {
		old any
		new any
	}
	updated := make(chan updatePair, 1)
	tc.OnUpdate(func(oldRow any, newRow any) {
		select {
		case updated <- updatePair{old: oldRow, new: newRow}:
		default:
		}
	})

	// Update the row.
	err = conn.CallReducer("update_unique_u_8", schema.UpdateUniqueU8Args(testKey, 222))
	require.NoError(t, err)

	select {
	case pair := <-updated:
		oldTyped, ok := pair.old.(*schema.KeyDataRow[uint8])
		require.True(t, ok)
		newTyped, ok := pair.new.(*schema.KeyDataRow[uint8])
		require.True(t, ok)

		assert.Equal(t, int32(111), oldTyped.Data)
		assert.Equal(t, int32(222), newTyped.Data)
		assert.Equal(t, testKey, oldTyped.Key)
		assert.Equal(t, testKey, newTyped.Key)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for update callback")
	}
}

func TestCache_Count(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	for i := uint32(0); i < 5; i++ {
		err := conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(i+1))
		require.NoError(t, err)
	}

	waitForCount(t, tc, baseline+5, defaultTimeout)
	assert.Equal(t, baseline+5, tc.Count())
}

func TestCache_Iter(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	expected := []uint32{10, 20, 30}
	for _, v := range expected {
		err := conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(v))
		require.NoError(t, err)
	}

	waitForCount(t, tc, baseline+3, defaultTimeout)

	// Collect all rows via Iter and check that our inserted values are present.
	collected := make(map[uint32]bool)
	tc.Iter(func(row any) bool {
		typed, ok := row.(*schema.Row[uint32])
		if ok {
			collected[typed.Value] = true
		}
		return true
	})

	for _, v := range expected {
		assert.True(t, collected[v], "expected value %d to be in cache", v)
	}
}

func TestCache_RemoveCallback(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	var callCount atomic.Int32
	cbID := tc.OnInsert(func(_ any) {
		callCount.Add(1)
	})

	// Insert a row -- callback should fire.
	err := conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(100))
	require.NoError(t, err)
	waitForCount(t, tc, baseline+1, defaultTimeout)
	assert.Equal(t, int32(1), callCount.Load(), "callback should have fired once")

	// Remove the callback.
	tc.RemoveCallback(cbID)

	// Insert another row -- callback should NOT fire.
	err = conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(200))
	require.NoError(t, err)
	waitForCount(t, tc, baseline+2, defaultTimeout)

	assert.Equal(t, int32(1), callCount.Load(), "callback should not fire after removal")
}
