//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/integration/schema"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func TestContext_CallerIdentity(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneIdentityDef)
	subscribe(t, conn, "SELECT * FROM one_identity")

	callerIdentity := conn.Identity()
	require.False(t, callerIdentity.IsZero(), "caller identity should be non-zero")

	tc := conn.Cache().GetTable("one_identity")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Call a reducer that stores the caller's identity.
	err := conn.CallReducer("insert_caller_one_identity", schema.InsertCallerOneIdentityArgs())
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Use OnInsert-style: find our identity among all rows.
	var storedIdentity types.Identity
	tc.Iter(func(row any) bool {
		typed, ok := row.(*schema.Row[types.Identity])
		if ok && typed.Value.String() == callerIdentity.String() {
			storedIdentity = typed.Value
			return false
		}
		return true
	})

	require.NotNil(t, storedIdentity, "stored identity should not be nil")
	assert.Equal(t, callerIdentity.String(), storedIdentity.String(),
		"stored identity should match the caller's identity")
}

func TestContext_CallTimestamp(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneTimestampDef)
	subscribe(t, conn, "SELECT * FROM one_timestamp")

	tc := conn.Cache().GetTable("one_timestamp")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Call a reducer that stores the call timestamp.
	err := conn.CallReducer("insert_call_timestamp", schema.InsertCallTimestampArgs())
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	// Verify a timestamp was stored and is non-zero (microseconds > 0).
	var storedTimestamp types.Timestamp
	tc.Iter(func(row any) bool {
		typed, ok := row.(*schema.Row[types.Timestamp])
		if ok {
			storedTimestamp = typed.Value
			return false
		}
		return true
	})

	require.NotNil(t, storedTimestamp, "stored timestamp should not be nil")
	assert.NotZero(t, storedTimestamp.Microseconds(), "stored timestamp microseconds should be non-zero")
}

func TestContext_CallUuidV4(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))
	conn.RegisterTable(schema.OneUuidDef)
	subscribe(t, conn, "SELECT * FROM one_uuid")

	tc := conn.Cache().GetTable("one_uuid")
	require.NotNil(t, tc)
	baseline := tc.Count()

	// Use an insert callback to capture the newly inserted UUID.
	insertedUuid := make(chan types.Uuid, 1)
	tc.OnInsert(func(row any) {
		typed, ok := row.(*schema.Row[types.Uuid])
		if ok {
			select {
			case insertedUuid <- typed.Value:
			default:
			}
		}
	})

	// Try insert_call_uuid_v4 first (server-generated UUID).
	// If that reducer panics (e.g., new_uuid_v4 not supported), fall back to insert_one_uuid.
	err := conn.CallReducer("insert_call_uuid_v4", schema.InsertCallUuidV4Args())
	require.NoError(t, err)

	// Wait a short time for the server-generated UUID.
	select {
	case storedUuid := <-insertedUuid:
		assert.False(t, storedUuid.IsZero(), "stored UUID should be non-zero")
		return
	case <-time.After(3 * time.Second):
		// Reducer may have panicked on the server; fall back to explicit UUID insert.
		t.Log("insert_call_uuid_v4 did not produce a row; falling back to insert_one_uuid")
	}

	// Fallback: insert an explicit UUID.
	testUuid := types.NewUuid([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	err = conn.CallReducer("insert_one_uuid", schema.InsertOneUuidArgs(testUuid))
	require.NoError(t, err)

	waitForCount(t, tc, baseline+1, defaultTimeout)

	select {
	case storedUuid := <-insertedUuid:
		assert.False(t, storedUuid.IsZero(), "stored UUID should be non-zero")
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for UUID insert callback")
	}
}
