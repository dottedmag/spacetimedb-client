//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
)

func TestMultiModule_ConnectAndSubscribe(t *testing.T) {
	for _, variant := range allVariants() {
		t.Run(variant, func(t *testing.T) {
			skipIfNoModule(t, variant)
			h := moduleHandle(t, variant)

			conn := connectAndRun(t, h.DatabaseName())
			conn.RegisterTable(schema.OneU32Def)

			subscribe(t, conn, "SELECT * FROM one_u_32")

			tc := conn.Cache().GetTable("one_u_32")
			require.NotNil(t, tc, "table cache should exist after subscribe on %s module", variant)

			assert.True(t, conn.IsActive())
			assert.False(t, conn.Identity().IsZero())
		})
	}
}

func TestMultiModule_InsertAllPrimitives(t *testing.T) {
	cases := []struct {
		name     string
		tableDef cache.TableDef
		reducer  string
		args     bsatn.Serializable
	}{
		{"u8", schema.OneU8Def, "insert_one_u_8", schema.InsertOneU8Args(42)},
		{"u16", schema.OneU16Def, "insert_one_u_16", schema.InsertOneU16Args(1000)},
		{"u32", schema.OneU32Def, "insert_one_u_32", schema.InsertOneU32Args(100000)},
		{"u64", schema.OneU64Def, "insert_one_u_64", schema.InsertOneU64Args(9999999)},
		{"i8", schema.OneI8Def, "insert_one_i_8", schema.InsertOneI8Args(-42)},
		{"i16", schema.OneI16Def, "insert_one_i_16", schema.InsertOneI16Args(-1000)},
		{"i32", schema.OneI32Def, "insert_one_i_32", schema.InsertOneI32Args(-100000)},
		{"i64", schema.OneI64Def, "insert_one_i_64", schema.InsertOneI64Args(-9999999)},
		{"bool", schema.OneBoolDef, "insert_one_bool", schema.InsertOneBoolArgs(true)},
		{"f32", schema.OneF32Def, "insert_one_f_32", schema.InsertOneF32Args(3.14)},
		{"f64", schema.OneF64Def, "insert_one_f_64", schema.InsertOneF64Args(2.71828)},
		{"string", schema.OneStringDef, "insert_one_string", schema.InsertOneStringArgs("hello")},
	}

	for _, variant := range allVariants() {
		t.Run(variant, func(t *testing.T) {
			skipIfNoModule(t, variant)
			h := moduleHandle(t, variant)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					conn := connectAndRun(t, h.DatabaseName())
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
		})
	}
}

func TestMultiModule_UniqueConstraints(t *testing.T) {
	for _, variant := range allVariants() {
		t.Run(variant, func(t *testing.T) {
			skipIfNoModule(t, variant)
			h := moduleHandle(t, variant)

			conn := connectAndRun(t, h.DatabaseName())
			conn.RegisterTable(schema.UniqueU8Def)
			subscribe(t, conn, "SELECT * FROM unique_u_8")

			tc := conn.Cache().GetTable("unique_u_8")
			require.NotNil(t, tc)
			baseline := tc.Count()

			// Use a unique key unlikely to conflict with other tests.
			const testKey = uint8(251)

			// Insert a unique row.
			err := conn.CallReducer("insert_unique_u_8", schema.InsertUniqueU8Args(testKey, 100))
			require.NoError(t, err)
			waitForCount(t, tc, baseline+1, defaultTimeout)

			// Update the row.
			updated := make(chan struct{}, 1)
			tc.OnUpdate(func(_ any, _ any) {
				select {
				case updated <- struct{}{}:
				default:
				}
			})

			err = conn.CallReducer("update_unique_u_8", schema.UpdateUniqueU8Args(testKey, 200))
			require.NoError(t, err)
			waitFor(t, updated, defaultTimeout)

			assert.Equal(t, baseline+1, tc.Count(), "count should still be baseline+1 after update")

			// Delete the row.
			deleted := make(chan struct{}, 1)
			tc.OnDelete(func(_ any) {
				select {
				case deleted <- struct{}{}:
				default:
				}
			})

			err = conn.CallReducer("delete_unique_u_8", schema.DeleteUniqueU8Args(testKey))
			require.NoError(t, err)
			waitFor(t, deleted, defaultTimeout)

			assert.Equal(t, baseline, tc.Count(), "table should be back to baseline after delete")
		})
	}
}

func TestMultiModule_PrimaryKeys(t *testing.T) {
	for _, variant := range allVariants() {
		t.Run(variant, func(t *testing.T) {
			skipIfNoModule(t, variant)
			h := moduleHandle(t, variant)

			conn := connectAndRun(t, h.DatabaseName())
			conn.RegisterTable(schema.PkU8Def)
			subscribe(t, conn, "SELECT * FROM pk_u_8")

			tc := conn.Cache().GetTable("pk_u_8")
			require.NotNil(t, tc)
			baseline := tc.Count()

			const testKey = uint8(251)

			// Insert a PK row.
			err := conn.CallReducer("insert_pk_u_8", schema.InsertPkU8Args(testKey, 100))
			require.NoError(t, err)
			waitForCount(t, tc, baseline+1, defaultTimeout)

			// Update the row.
			updated := make(chan struct{}, 1)
			tc.OnUpdate(func(_ any, _ any) {
				select {
				case updated <- struct{}{}:
				default:
				}
			})

			err = conn.CallReducer("update_pk_u_8", schema.UpdatePkU8Args(testKey, 200))
			require.NoError(t, err)
			waitFor(t, updated, defaultTimeout)

			assert.Equal(t, baseline+1, tc.Count(), "count should still be baseline+1 after update")

			// Delete the row.
			deleted := make(chan struct{}, 1)
			tc.OnDelete(func(_ any) {
				select {
				case deleted <- struct{}{}:
				default:
				}
			})

			err = conn.CallReducer("delete_pk_u_8", schema.DeletePkU8Args(testKey))
			require.NoError(t, err)
			waitFor(t, deleted, defaultTimeout)

			assert.Equal(t, baseline, tc.Count(), "table should be back to baseline after delete")
		})
	}
}
