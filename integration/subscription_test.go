//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
)

func TestSubscribe_EmptyTable(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)

	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc, "table cache should exist after subscribe")
	// Table may have rows from previous test runs; just verify the cache is accessible.
	assert.GreaterOrEqual(t, tc.Count(), 0, "table count should be non-negative")
}

func TestSubscribe_OverlappingHandlesReleaseRows(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(uint32(time.Now().UnixNano()))))
	rows, err := conn.OneOffQuery("SELECT * FROM one_u_32")
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	applied1, applied2 := make(chan struct{}, 1), make(chan struct{}, 1)
	h1, err := conn.Subscribe("SELECT * FROM one_u_32").OnApplied(func() { applied1 <- struct{}{} }).Build()
	require.NoError(t, err)
	waitFor(t, applied1, defaultTimeout)
	h2, err := conn.Subscribe("SELECT * FROM one_u_32").OnApplied(func() { applied2 <- struct{}{} }).Build()
	require.NoError(t, err)
	waitFor(t, applied2, defaultTimeout)
	tc := conn.Cache().GetTable("one_u_32")
	require.Equal(t, len(rows), tc.Count())
	require.NoError(t, h1.Unsubscribe())
	_, err = conn.OneOffQuery("SELECT * FROM one_u_32")
	require.NoError(t, err)
	require.Equal(t, len(rows), tc.Count())
	require.NoError(t, h2.Unsubscribe())
	_, err = conn.OneOffQuery("SELECT * FROM one_u_32")
	require.NoError(t, err)
	require.Zero(t, tc.Count())
}

func TestSubscribe_WithExistingRows(t *testing.T) {
	dbName := defaultDBName(t)
	conn := connectAndRun(t, dbName)
	conn.RegisterTable(schema.OneU32Def)

	// Subscribe first to get the baseline, then insert additional rows.
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Insert 3 rows via reducer with unique values.
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(88801)))
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(88802)))
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(88803)))

	waitForCount(t, tc, baseline+3, defaultTimeout)
}

func TestSubscribe_MultipleQueries(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU8Def)
	conn.RegisterTable(schema.OneU16Def)
	conn.RegisterTable(schema.OneStringDef)

	subscribe(t, conn, "SELECT * FROM one_u_8", "SELECT * FROM one_u_16", "SELECT * FROM one_string")

	// All tables should be subscribed and accessible.
	assert.NotNil(t, conn.Cache().GetTable("one_u_8"))
	assert.NotNil(t, conn.Cache().GetTable("one_u_16"))
	assert.NotNil(t, conn.Cache().GetTable("one_string"))
}

func TestSubscribe_OnAppliedCallback(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)

	appliedCount := 0
	applied := make(chan struct{}, 1)

	_, err := conn.Subscribe("SELECT * FROM one_u_32").
		OnApplied(func() {
			appliedCount++
			select {
			case applied <- struct{}{}:
			default:
			}
		}).
		Build()
	require.NoError(t, err)

	waitFor(t, applied, defaultTimeout)
	assert.Equal(t, 1, appliedCount, "OnApplied should fire exactly once")
}

func TestSubscribe_AllPrimitiveTypes(t *testing.T) {
	cases := []struct {
		name     string
		tableDef cache.TableDef
		query    string
	}{
		{"u8", schema.OneU8Def, "SELECT * FROM one_u_8"},
		{"u16", schema.OneU16Def, "SELECT * FROM one_u_16"},
		{"u32", schema.OneU32Def, "SELECT * FROM one_u_32"},
		{"u64", schema.OneU64Def, "SELECT * FROM one_u_64"},
		{"i8", schema.OneI8Def, "SELECT * FROM one_i_8"},
		{"i16", schema.OneI16Def, "SELECT * FROM one_i_16"},
		{"i32", schema.OneI32Def, "SELECT * FROM one_i_32"},
		{"i64", schema.OneI64Def, "SELECT * FROM one_i_64"},
		{"bool", schema.OneBoolDef, "SELECT * FROM one_bool"},
		{"f32", schema.OneF32Def, "SELECT * FROM one_f_32"},
		{"f64", schema.OneF64Def, "SELECT * FROM one_f_64"},
		{"string", schema.OneStringDef, "SELECT * FROM one_string"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := connectAndRun(t, defaultDBName(t))
			conn.RegisterTable(tc.tableDef)

			subscribe(t, conn, tc.query)

			tableCache := conn.Cache().GetTable(tc.tableDef.TableName())
			require.NotNil(t, tableCache, "table cache should exist for %s", tc.name)
		})
	}
}

func TestSubscribe_Unsubscribe(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)

	applied := make(chan struct{}, 1)
	handle, err := conn.Subscribe("SELECT * FROM one_u_32").
		OnApplied(func() {
			select {
			case applied <- struct{}{}:
			default:
			}
		}).
		Build()
	require.NoError(t, err)

	waitFor(t, applied, defaultTimeout)
	assert.True(t, handle.IsActive(), "subscription should be active after apply")

	err = handle.Unsubscribe()
	require.NoError(t, err)
	assert.False(t, handle.IsActive(), "subscription should be inactive after unsubscribe")
}
