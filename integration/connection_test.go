//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/client"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func TestConnect_ReceivesIdentityAndToken(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))

	assert.False(t, conn.Identity().IsZero(), "identity should be non-zero after connect")
	assert.NotEmpty(t, conn.Token(), "token should be non-empty after connect")
}

func TestConnect_IsActive(t *testing.T) {
	conn := connectAndRun(t, defaultDBName(t))

	assert.True(t, conn.IsActive(), "connection should be active after connect")
}

func TestConnect_Disconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	disconnected := make(chan struct{}, 1)
	connected := make(chan struct{}, 1)

	builder := testHarness.NewConnection(defaultDBName(t)).
		OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}).
		OnDisconnect(func(_ client.DbConnection, _ error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		})

	c, err := builder.Build(ctx)
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx)
	}()

	waitFor(t, connected, defaultTimeout)

	err = c.Disconnect()
	require.NoError(t, err)

	select {
	case <-disconnected:
		// expected
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for OnDisconnect callback")
	}

	select {
	case runErr := <-runDone:
		// Run() should return nil on clean disconnect.
		assert.NoError(t, runErr)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for Run() to return")
	}
}

func TestConnect_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	connected := make(chan struct{}, 1)
	disconnected := make(chan struct{}, 1)

	builder := testHarness.NewConnection(defaultDBName(t)).
		OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}).
		OnDisconnect(func(_ client.DbConnection, _ error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		})

	c, err := builder.Build(ctx)
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx)
	}()

	waitFor(t, connected, defaultTimeout)

	cancel()

	select {
	case <-disconnected:
		// expected
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for OnDisconnect after context cancel")
	}

	select {
	case runErr := <-runDone:
		assert.ErrorIs(t, runErr, context.Canceled)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for Run() to return after context cancel")
	}
}

func TestConnect_BadUri(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	_, err := client.NewDbConnection().
		WithUri("ws://localhost:99999").
		WithDatabaseName("nonexistent").
		OnConnectError(func(e error) {
			errCh <- e
		}).
		Build(ctx)

	// Build itself may return error, or OnConnectError fires.
	if err != nil {
		return // expected
	}

	select {
	case <-errCh:
		// expected
	case <-time.After(5 * time.Second):
		t.Fatal("expected connection error")
	}
}

func TestConnect_TokenReuse(t *testing.T) {
	dbName := defaultDBName(t)

	// First connection: get identity and token.
	conn1 := connectAndRun(t, dbName)

	identity1 := conn1.Identity()
	token1 := conn1.Token()
	require.False(t, identity1.IsZero())
	require.NotEmpty(t, token1)

	err := conn1.Disconnect()
	require.NoError(t, err)

	// Give the server a moment to process disconnect.
	time.Sleep(500 * time.Millisecond)

	// Second connection: reuse the token.
	conn2 := connectAndRun(t, dbName, func(b client.DbConnectionBuilder) client.DbConnectionBuilder {
		return b.WithToken(token1)
	})

	identity2 := conn2.Identity()
	require.False(t, identity2.IsZero())

	assert.Equal(t, identity1.String(), identity2.String(),
		"reconnecting with the same token should yield the same identity")
}

func TestConnect_MultipleConnections(t *testing.T) {
	dbName := defaultDBName(t)

	conn1 := connectAndRun(t, dbName)
	conn2 := connectAndRun(t, dbName)
	conn3 := connectAndRun(t, dbName)

	require.False(t, conn1.ConnectionId().IsZero())
	require.False(t, conn2.ConnectionId().IsZero())
	require.False(t, conn3.ConnectionId().IsZero())

	id1 := conn1.ConnectionId().String()
	id2 := conn2.ConnectionId().String()
	id3 := conn3.ConnectionId().String()

	assert.NotEqual(t, id1, id2, "connection IDs should be unique")
	assert.NotEqual(t, id1, id3, "connection IDs should be unique")
	assert.NotEqual(t, id2, id3, "connection IDs should be unique")
}
