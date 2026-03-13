package cache_test

import (
	"sync/atomic"
	"testing"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRow is a simple row type for testing.
type testRow struct {
	ID   uint32
	Name string
}

// testTableDef implements cache.TableDef for testing.
type testTableDef struct {
	name string
}

func (d *testTableDef) TableName() string { return d.name }

func (d *testTableDef) DecodeRow(r bsatn.Reader) (any, error) {
	id, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	name, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &testRow{ID: id, Name: name}, nil
}

func (d *testTableDef) EncodeRow(row any) []byte {
	tr := row.(*testRow)
	w := bsatn.NewWriter(32)
	w.PutU32(tr.ID)
	w.PutString(tr.Name)
	return w.Bytes()
}

// testTableDefWithPK implements cache.TableDefWithPK for testing.
type testTableDefWithPK struct {
	testTableDef
}

func (d *testTableDefWithPK) PrimaryKey(row any) any {
	return row.(*testRow).ID
}

func encodeTestRow(id uint32, name string) []byte {
	w := bsatn.NewWriter(32)
	w.PutU32(id)
	w.PutString(name)
	return w.Bytes()
}

func decodeRow(def cache.TableDef, rowBytes []byte) (any, error) {
	r := bsatn.NewReader(rowBytes)
	return def.DecodeRow(r)
}

// --- ClientCache tests ---

func TestClientCache_RegisterTable_GetTable(t *testing.T) {
	cc := cache.NewClientCache()

	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc, "GetTable should return a non-nil TableCache after RegisterTable")

	assert.Equal(t, 0, tc.Count(), "new table should have 0 rows")
}

func TestClientCache_GetTable_NotFound(t *testing.T) {
	cc := cache.NewClientCache()

	tc := cc.GetTable("nonexistent")
	assert.Nil(t, tc, "GetTable should return nil for unregistered table")
}

func TestClientCache_RegisterTable_MultipleTables(t *testing.T) {
	cc := cache.NewClientCache()

	cc.RegisterTable(&testTableDef{name: "users"})
	cc.RegisterTable(&testTableDef{name: "items"})
	cc.RegisterTable(&testTableDef{name: "events"})

	assert.NotNil(t, cc.GetTable("users"))
	assert.NotNil(t, cc.GetTable("items"))
	assert.NotNil(t, cc.GetTable("events"))
	assert.Nil(t, cc.GetTable("other"))
}

func TestClientCache_DecodeRow(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	rowBytes := encodeTestRow(42, "alice")
	r := bsatn.NewReader(rowBytes)
	row, err := cc.DecodeRow("users", r)
	require.NoError(t, err)
	require.NotNil(t, row)

	tr := row.(*testRow)
	assert.Equal(t, uint32(42), tr.ID)
	assert.Equal(t, "alice", tr.Name)
}

func TestClientCache_DecodeRow_UnknownTable(t *testing.T) {
	cc := cache.NewClientCache()

	rowBytes := encodeTestRow(1, "bob")
	r := bsatn.NewReader(rowBytes)
	row, err := cc.DecodeRow("nonexistent", r)
	assert.NoError(t, err)
	assert.Nil(t, row)
}

func TestClientCache_GetTableDef(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDefWithPK{testTableDef: testTableDef{name: "users"}}
	cc.RegisterTable(def)

	pkDef, hasPK := cc.GetTableDef("users")
	assert.True(t, hasPK)
	require.NotNil(t, pkDef)
}

func TestClientCache_GetTableDef_NoPK(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	_, hasPK := cc.GetTableDef("users")
	assert.False(t, hasPK)
}

func TestClientCache_GetTableDef_NotFound(t *testing.T) {
	cc := cache.NewClientCache()

	pkDef, hasPK := cc.GetTableDef("nonexistent")
	assert.False(t, hasPK)
	assert.Nil(t, pkDef)
}

// --- TableCache ApplyInsert tests ---

func TestTableCache_ApplyInsert(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)
	assert.Equal(t, 1, tc.Count())
}

func TestTableCache_ApplyInsert_MultipleRows(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	for i := uint32(0); i < 5; i++ {
		rowBytes := encodeTestRow(i, "user")
		row, err := decodeRow(def, rowBytes)
		require.NoError(t, err)
		tc.ApplyInsert(rowBytes, row)
	}

	assert.Equal(t, 5, tc.Count())
}

// --- TableCache ApplyDelete tests ---

func TestTableCache_ApplyDelete(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)
	require.Equal(t, 1, tc.Count())

	tc.ApplyDelete(rowBytes, row)
	assert.Equal(t, 0, tc.Count())
}

func TestTableCache_ApplyDelete_NonExistent(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	// Deleting a non-existent row should not panic
	tc.ApplyDelete(rowBytes, row)
	assert.Equal(t, 0, tc.Count())
}

// --- TableCache ApplyUpdate tests ---

func TestTableCache_ApplyUpdate(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	oldBytes := encodeTestRow(1, "alice")
	oldRow, err := decodeRow(def, oldBytes)
	require.NoError(t, err)
	tc.ApplyInsert(oldBytes, oldRow)
	require.Equal(t, 1, tc.Count())

	newBytes := encodeTestRow(1, "alice_renamed")
	newRow, err := decodeRow(def, newBytes)
	require.NoError(t, err)

	tc.ApplyUpdate(oldBytes, oldRow, newBytes, newRow)
	assert.Equal(t, 1, tc.Count())

	// Verify the updated row is in the cache
	var found *testRow
	tc.Iter(func(row any) bool {
		found = row.(*testRow)
		return false
	})
	require.NotNil(t, found)
	assert.Equal(t, "alice_renamed", found.Name)
}

// --- TableCache Count and Iter tests ---

func TestTableCache_Count(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "items"}
	cc.RegisterTable(def)

	tc := cc.GetTable("items")
	require.NotNil(t, tc)

	row1Bytes := encodeTestRow(1, "sword")
	row1, err := decodeRow(def, row1Bytes)
	require.NoError(t, err)
	tc.ApplyInsert(row1Bytes, row1)

	row2Bytes := encodeTestRow(2, "shield")
	row2, err := decodeRow(def, row2Bytes)
	require.NoError(t, err)
	tc.ApplyInsert(row2Bytes, row2)

	assert.Equal(t, 2, tc.Count())
}

func TestTableCache_Iter(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "items"}
	cc.RegisterTable(def)

	tc := cc.GetTable("items")
	require.NotNil(t, tc)

	row1Bytes := encodeTestRow(1, "sword")
	row1, err := decodeRow(def, row1Bytes)
	require.NoError(t, err)
	tc.ApplyInsert(row1Bytes, row1)

	row2Bytes := encodeTestRow(2, "shield")
	row2, err := decodeRow(def, row2Bytes)
	require.NoError(t, err)
	tc.ApplyInsert(row2Bytes, row2)

	var collected []*testRow
	tc.Iter(func(row any) bool {
		collected = append(collected, row.(*testRow))
		return true
	})

	assert.Len(t, collected, 2)

	ids := map[uint32]bool{}
	for _, r := range collected {
		ids[r.ID] = true
	}
	assert.True(t, ids[1], "should contain row with ID 1")
	assert.True(t, ids[2], "should contain row with ID 2")
}

func TestTableCache_Iter_EarlyStop(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "items"}
	cc.RegisterTable(def)

	tc := cc.GetTable("items")
	require.NotNil(t, tc)

	for i := uint32(1); i <= 3; i++ {
		rowBytes := encodeTestRow(i, "item")
		row, err := decodeRow(def, rowBytes)
		require.NoError(t, err)
		tc.ApplyInsert(rowBytes, row)
	}

	count := 0
	tc.Iter(func(row any) bool {
		count++
		return false // stop after first
	})

	assert.Equal(t, 1, count, "iteration should stop after returning false")
}

// --- Callback tests ---

func TestTableCache_OnInsert_Callback(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var insertedRow atomic.Value
	tc.OnInsert(func(row any) {
		insertedRow.Store(row)
	})

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)

	stored := insertedRow.Load()
	require.NotNil(t, stored, "insert callback should have fired")
	tr := stored.(*testRow)
	assert.Equal(t, uint32(1), tr.ID)
	assert.Equal(t, "alice", tr.Name)
}

func TestTableCache_OnDelete_Callback(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var deletedRow atomic.Value
	tc.OnDelete(func(row any) {
		deletedRow.Store(row)
	})

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)
	require.Equal(t, 1, tc.Count())

	tc.ApplyDelete(rowBytes, row)

	stored := deletedRow.Load()
	require.NotNil(t, stored, "delete callback should have fired")
	tr := stored.(*testRow)
	assert.Equal(t, uint32(1), tr.ID)
	assert.Equal(t, "alice", tr.Name)
	assert.Equal(t, 0, tc.Count())
}

func TestTableCache_OnUpdate_Callback(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var oldVal, newVal atomic.Value
	tc.OnUpdate(func(oldRow any, newRow any) {
		oldVal.Store(oldRow)
		newVal.Store(newRow)
	})

	oldBytes := encodeTestRow(1, "alice")
	oldRow, err := decodeRow(def, oldBytes)
	require.NoError(t, err)
	tc.ApplyInsert(oldBytes, oldRow)

	newBytes := encodeTestRow(1, "alice_v2")
	newRow, err := decodeRow(def, newBytes)
	require.NoError(t, err)

	tc.ApplyUpdate(oldBytes, oldRow, newBytes, newRow)

	storedOld := oldVal.Load()
	storedNew := newVal.Load()
	require.NotNil(t, storedOld, "update callback should have fired (old)")
	require.NotNil(t, storedNew, "update callback should have fired (new)")
	assert.Equal(t, "alice", storedOld.(*testRow).Name)
	assert.Equal(t, "alice_v2", storedNew.(*testRow).Name)
}

func TestTableCache_RemoveCallback(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var callCount atomic.Int32
	cbID := tc.OnInsert(func(row any) {
		callCount.Add(1)
	})

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)
	assert.Equal(t, int32(1), callCount.Load())

	tc.RemoveCallback(cbID)

	row2Bytes := encodeTestRow(2, "bob")
	row2, err := decodeRow(def, row2Bytes)
	require.NoError(t, err)

	tc.ApplyInsert(row2Bytes, row2)
	assert.Equal(t, int32(1), callCount.Load(), "callback should not fire after removal")
}

func TestTableCache_MultipleInsertCallbacks(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var count1, count2 atomic.Int32
	tc.OnInsert(func(row any) {
		count1.Add(1)
	})
	tc.OnInsert(func(row any) {
		count2.Add(1)
	})

	rowBytes := encodeTestRow(1, "alice")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)

	assert.Equal(t, int32(1), count1.Load(), "first callback should fire")
	assert.Equal(t, int32(1), count2.Load(), "second callback should fire")
}

// --- Concurrent access tests ---

func TestClientCache_ConcurrentInserts(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "users"}
	cc.RegisterTable(def)

	tc := cc.GetTable("users")
	require.NotNil(t, tc)

	var insertCount atomic.Int32
	tc.OnInsert(func(row any) {
		insertCount.Add(1)
	})

	const numGoroutines = 10
	const rowsPerGoroutine = 50
	done := make(chan struct{}, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < rowsPerGoroutine; i++ {
				id := uint32(goroutineID*rowsPerGoroutine + i)
				rowBytes := encodeTestRow(id, "user")
				row, err := decodeRow(def, rowBytes)
				if err != nil {
					return
				}
				tc.ApplyInsert(rowBytes, row)
			}
		}(g)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	assert.Equal(t, numGoroutines*rowsPerGoroutine, tc.Count())
	assert.Equal(t, int32(numGoroutines*rowsPerGoroutine), insertCount.Load())
}

func TestClientCache_ConcurrentRegisterAndGet(t *testing.T) {
	cc := cache.NewClientCache()

	const numTables = 50
	done := make(chan struct{}, numTables*2)

	for i := 0; i < numTables; i++ {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			name := "table_" + string(rune('A'+idx%26)) + string(rune('0'+idx/26))
			cc.RegisterTable(&testTableDef{name: name})
		}(i)
	}

	for i := 0; i < numTables; i++ {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			name := "table_" + string(rune('A'+idx%26)) + string(rune('0'+idx/26))
			_ = cc.GetTable(name)
		}(i)
	}

	for i := 0; i < numTables*2; i++ {
		<-done
	}

	for i := 0; i < numTables; i++ {
		name := "table_" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		assert.NotNil(t, cc.GetTable(name), "table %s should be registered", name)
	}
}

func TestTableCache_ConcurrentCallbackRegistration(t *testing.T) {
	cc := cache.NewClientCache()
	def := &testTableDef{name: "items"}
	cc.RegisterTable(def)

	tc := cc.GetTable("items")
	require.NotNil(t, tc)

	const numCallbacks = 100
	var totalInserts atomic.Int32
	done := make(chan struct{}, numCallbacks)

	for i := 0; i < numCallbacks; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			tc.OnInsert(func(row any) {
				totalInserts.Add(1)
			})
		}()
	}

	for i := 0; i < numCallbacks; i++ {
		<-done
	}

	rowBytes := encodeTestRow(1, "sword")
	row, err := decodeRow(def, rowBytes)
	require.NoError(t, err)

	tc.ApplyInsert(rowBytes, row)

	assert.Equal(t, int32(numCallbacks), totalInserts.Load(),
		"all %d callbacks should have fired", numCallbacks)
}
