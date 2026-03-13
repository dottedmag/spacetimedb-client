//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func TestReducer_InsertPrimitive(t *testing.T) {
	cases := []struct {
		name     string
		tableDef cache.TableDef
		reducer  string
		args     bsatn.Serializable
	}{
		{"u8", schema.OneU8Def, "insert_one_u_8", schema.InsertOneU8Args(99)},
		{"u16", schema.OneU16Def, "insert_one_u_16", schema.InsertOneU16Args(2222)},
		{"u32", schema.OneU32Def, "insert_one_u_32", schema.InsertOneU32Args(222222)},
		{"u64", schema.OneU64Def, "insert_one_u_64", schema.InsertOneU64Args(8888888)},
		{"i8", schema.OneI8Def, "insert_one_i_8", schema.InsertOneI8Args(-99)},
		{"i16", schema.OneI16Def, "insert_one_i_16", schema.InsertOneI16Args(-2222)},
		{"i32", schema.OneI32Def, "insert_one_i_32", schema.InsertOneI32Args(-222222)},
		{"i64", schema.OneI64Def, "insert_one_i_64", schema.InsertOneI64Args(-8888888)},
		{"bool", schema.OneBoolDef, "insert_one_bool", schema.InsertOneBoolArgs(false)},
		{"f32", schema.OneF32Def, "insert_one_f_32", schema.InsertOneF32Args(1.23)},
		{"f64", schema.OneF64Def, "insert_one_f_64", schema.InsertOneF64Args(4.56789)},
		{"string", schema.OneStringDef, "insert_one_string", schema.InsertOneStringArgs("reducer_test")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := connectAndRun(t, defaultDBName(t))
			conn.RegisterTable(tc.tableDef)
			subscribe(t, conn, "SELECT * FROM "+tc.tableDef.TableName())

			tableCache := conn.Cache().GetTable(tc.tableDef.TableName())
			require.NotNil(t, tableCache)
			baseline := tableCache.Count()

			err := conn.CallReducer(tc.reducer, tc.args)
			require.NoError(t, err)

			waitForCount(t, tableCache, baseline+1, defaultTimeout)
		})
	}
}

func TestReducer_InsertVec(t *testing.T) {
	cases := []struct {
		name     string
		tableDef cache.TableDef
		reducer  string
		args     bsatn.Serializable
	}{
		{"vec_u_8", schema.VecU8Def, "insert_vec_u_8", schema.InsertVecU8Args([]uint8{1, 2, 3})},
		{"vec_u_32", schema.VecU32Def, "insert_vec_u_32", schema.InsertVecU32Args([]uint32{10, 20, 30})},
		{"vec_string", schema.VecStringDef, "insert_vec_string", schema.InsertVecStringArgs([]string{"a", "b", "c"})},
		{"vec_bool", schema.VecBoolDef, "insert_vec_bool", schema.InsertVecBoolArgs([]bool{true, false, true})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := connectAndRun(t, defaultDBName(t))
			conn.RegisterTable(tc.tableDef)
			subscribe(t, conn, "SELECT * FROM "+tc.tableDef.TableName())

			tableCache := conn.Cache().GetTable(tc.tableDef.TableName())
			require.NotNil(t, tableCache)
			baseline := tableCache.Count()

			err := conn.CallReducer(tc.reducer, tc.args)
			require.NoError(t, err)

			waitForCount(t, tableCache, baseline+1, defaultTimeout)
		})
	}
}

func TestReducer_InsertOption_SomeAndNone(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OptionI32Def)
	subscribe(t, conn, "SELECT * FROM option_i_32")

	tc := conn.Cache().GetTable("option_i_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Insert Some value.
	someVal := int32(42)
	err := conn.CallReducer("insert_option_i_32", schema.InsertOptionI32Args(&someVal))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Insert None value.
	err = conn.CallReducer("insert_option_i_32", schema.InsertOptionI32Args(nil))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+2, defaultTimeout)
}

func TestReducer_UpdateUnique(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.UniqueU8Def)
	subscribe(t, conn, "SELECT * FROM unique_u_8")

	tc := conn.Cache().GetTable("unique_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(241)

	// Insert initial row.
	err := conn.CallReducer("insert_unique_u_8", schema.InsertUniqueU8Args(testKey, 100))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Register update callback.
	updated := make(chan struct{}, 1)
	var oldRow, newRow any
	tc.OnUpdate(func(old any, new any) {
		oldRow = old
		newRow = new
		select {
		case updated <- struct{}{}:
		default:
		}
	})

	// Update the row.
	err = conn.CallReducer("update_unique_u_8", schema.UpdateUniqueU8Args(testKey, 200))
	require.NoError(t, err)

	waitFor(t, updated, defaultTimeout)
	require.NotNil(t, oldRow, "old row should not be nil")
	require.NotNil(t, newRow, "new row should not be nil")

	oldTyped, ok := oldRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok, "old row should be *KeyDataRow[uint8]")
	newTyped, ok := newRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok, "new row should be *KeyDataRow[uint8]")

	assert.Equal(t, testKey, oldTyped.Key)
	assert.Equal(t, int32(100), oldTyped.Data)
	assert.Equal(t, testKey, newTyped.Key)
	assert.Equal(t, int32(200), newTyped.Data)
}

func TestReducer_DeleteUnique(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.UniqueU8Def)
	subscribe(t, conn, "SELECT * FROM unique_u_8")

	tc := conn.Cache().GetTable("unique_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(242)

	// Insert a row.
	err := conn.CallReducer("insert_unique_u_8", schema.InsertUniqueU8Args(testKey, 500))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Register delete callback.
	deleted := make(chan struct{}, 1)
	var deletedRow any
	tc.OnDelete(func(row any) {
		deletedRow = row
		select {
		case deleted <- struct{}{}:
		default:
		}
	})

	// Delete the row.
	err = conn.CallReducer("delete_unique_u_8", schema.DeleteUniqueU8Args(testKey))
	require.NoError(t, err)

	waitFor(t, deleted, defaultTimeout)
	require.NotNil(t, deletedRow)

	typed, ok := deletedRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok, "deleted row should be *KeyDataRow[uint8]")
	assert.Equal(t, testKey, typed.Key)
	assert.Equal(t, int32(500), typed.Data)

	assert.Equal(t, baseline, tc.Count(), "table should be back to baseline after delete")
}

func TestReducer_UpdatePk(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.PkU8Def)
	subscribe(t, conn, "SELECT * FROM pk_u_8")

	tc := conn.Cache().GetTable("pk_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(243)

	// Insert initial row.
	err := conn.CallReducer("insert_pk_u_8", schema.InsertPkU8Args(testKey, 100))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Register update callback.
	updated := make(chan struct{}, 1)
	var oldRow, newRow any
	tc.OnUpdate(func(old any, new any) {
		oldRow = old
		newRow = new
		select {
		case updated <- struct{}{}:
		default:
		}
	})

	// Update the row.
	err = conn.CallReducer("update_pk_u_8", schema.UpdatePkU8Args(testKey, 200))
	require.NoError(t, err)

	waitFor(t, updated, defaultTimeout)
	require.NotNil(t, oldRow)
	require.NotNil(t, newRow)

	oldTyped, ok := oldRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok)
	newTyped, ok := newRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok)

	assert.Equal(t, testKey, oldTyped.Key)
	assert.Equal(t, int32(100), oldTyped.Data)
	assert.Equal(t, testKey, newTyped.Key)
	assert.Equal(t, int32(200), newTyped.Data)
}

func TestReducer_DeletePk(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.PkU8Def)
	subscribe(t, conn, "SELECT * FROM pk_u_8")

	tc := conn.Cache().GetTable("pk_u_8")
	require.NotNil(t, tc)
	baseline := tc.Count()

	const testKey = uint8(244)

	// Insert a row.
	err := conn.CallReducer("insert_pk_u_8", schema.InsertPkU8Args(testKey, 700))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Register delete callback.
	deleted := make(chan struct{}, 1)
	var deletedRow any
	tc.OnDelete(func(row any) {
		deletedRow = row
		select {
		case deleted <- struct{}{}:
		default:
		}
	})

	// Delete the row.
	err = conn.CallReducer("delete_pk_u_8", schema.DeletePkU8Args(testKey))
	require.NoError(t, err)

	waitFor(t, deleted, defaultTimeout)
	require.NotNil(t, deletedRow)

	typed, ok := deletedRow.(*schema.KeyDataRow[uint8])
	require.True(t, ok)
	assert.Equal(t, testKey, typed.Key)
	assert.Equal(t, int32(700), typed.Data)

	assert.Equal(t, baseline, tc.Count(), "table should be back to baseline after delete")
}

func TestReducer_LargeTable(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.LargeTableDef)
	subscribe(t, conn, "SELECT * FROM large_table")

	largeRow := &schema.LargeTable{
		A: 1, B: 2, C: 3, D: 4,
		E: types.NewUint128(0, 0),
		F: types.NewUint256([32]byte{}),
		G: -1, H: -2, I: -3, J: -4,
		K: types.NewInt128(0, 0),
		L: types.NewInt256([32]byte{}),
		M: true, N: 1.5, O: 2.5, P: "large",
		Q: schema.SimpleEnumZero,
		R: schema.NewEnumU8(0),
		S: schema.UnitStruct{},
		T: schema.ByteStruct{},
		U: schema.EveryPrimitiveStruct{
			E: types.NewUint128(0, 0), F: types.NewUint256([32]byte{}),
			K: types.NewInt128(0, 0), L: types.NewInt256([32]byte{}),
			Q: types.NewIdentity([32]byte{}), R: types.NewConnectionId([16]byte{}),
			S: types.NewTimestamp(0), T: types.NewTimeDuration(0),
			U: types.NewUuid([16]byte{}),
		},
		V: schema.EveryVecStruct{},
	}

	tc := conn.Cache().GetTable("large_table")
	require.NotNil(t, tc)
	baseline := tc.Count()

	err := conn.CallReducer("insert_large_table", schema.InsertLargeTableArgs(largeRow))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	assert.Equal(t, baseline+1, tc.Count())
}
