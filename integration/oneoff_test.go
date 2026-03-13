//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
)

func TestOneOffQuery_SelectAll(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneU32Def)

	// Subscribe first to get baseline count.
	subscribe(t, conn, "SELECT * FROM one_u_32")

	tc := conn.Cache().GetTable("one_u_32")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Insert some rows with unique values for this test.
	const val1 = uint32(77701)
	const val2 = uint32(77702)
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(val1)))
	require.NoError(t, conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(val2)))

	waitForCount(t, tc, baseline+2, defaultTimeout)

	// One-off query.
	rows, err := conn.OneOffQuery("SELECT * FROM one_u_32")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), baseline+2, "one-off query should return at least baseline+2 rows")

	// Decode and verify our inserted values are present.
	found1, found2 := false, false
	for _, rowBytes := range rows {
		r := bsatn.NewReader(rowBytes)
		row, err := schema.OneU32Def.DecodeRow(r)
		require.NoError(t, err)
		typed, ok := row.(*schema.Row[uint32])
		require.True(t, ok)
		if typed.Value == val1 {
			found1 = true
		}
		if typed.Value == val2 {
			found2 = true
		}
	}
	assert.True(t, found1, "should find value %d", val1)
	assert.True(t, found2, "should find value %d", val2)
}

func TestOneOffQuery_EmptyResult(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneI64Def)
	subscribe(t, conn, "SELECT * FROM one_i_64")

	tc := conn.Cache().GetTable("one_i_64")
	require.NotNil(t, tc)

	// One-off query should return the same number of rows as the cache.
	rows, err := conn.OneOffQuery("SELECT * FROM one_i_64")
	require.NoError(t, err)
	assert.Equal(t, tc.Count(), len(rows), "one-off query should return same count as cache")
}

func TestOneOffQuery_AfterInsert(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneStringDef)
	subscribe(t, conn, "SELECT * FROM one_string")

	tc := conn.Cache().GetTable("one_string")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Insert rows.
	require.NoError(t, conn.CallReducer("insert_one_string", schema.InsertOneStringArgs("alpha")))
	require.NoError(t, conn.CallReducer("insert_one_string", schema.InsertOneStringArgs("beta")))
	require.NoError(t, conn.CallReducer("insert_one_string", schema.InsertOneStringArgs("gamma")))

	waitForCount(t, tc, baseline+3, defaultTimeout)

	// Short delay for server-side consistency.
	time.Sleep(200 * time.Millisecond)

	rows, err := conn.OneOffQuery("SELECT * FROM one_string")
	require.NoError(t, err)
	assert.Equal(t, baseline+3, len(rows), "should return baseline+3 rows after inserts")

	// Decode and collect values.
	values := make(map[string]bool)
	for _, rowBytes := range rows {
		r := bsatn.NewReader(rowBytes)
		row, err := schema.OneStringDef.DecodeRow(r)
		require.NoError(t, err)
		typed, ok := row.(*schema.Row[string])
		require.True(t, ok)
		values[typed.Value] = true
	}

	assert.True(t, values["alpha"])
	assert.True(t, values["beta"])
	assert.True(t, values["gamma"])
}

func TestOneOffQuery_InvalidSQL(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))

	_, err := conn.OneOffQuery("THIS IS NOT VALID SQL AT ALL")
	assert.Error(t, err, "invalid SQL should return an error")
}
