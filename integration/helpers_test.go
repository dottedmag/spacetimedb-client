//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/client"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/integration/harness"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// defaultTimeout is the default timeout for waiting on async callbacks.
const defaultTimeout = 10 * time.Second

// connectAndRun builds a connection, starts Run() in a background goroutine,
// and returns it. The connection is automatically disconnected when the test ends.
func connectAndRun(t *testing.T, dbName string, opts ...func(client.DbConnectionBuilder) client.DbConnectionBuilder) client.DbConnection {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	connected := make(chan struct{}, 1)
	var conn client.DbConnection

	builder := testHarness.NewConnection(dbName).
		OnConnect(func(c client.DbConnection, _ types.Identity, _ string) {
			conn = c
			select {
			case connected <- struct{}{}:
			default:
			}
		}).
		OnConnectError(func(err error) {
			t.Errorf("connect error: %v", err)
			cancel()
		}).
		OnReducerError(func(err error) {
			// A reducer call failing on the server (e.g. an unsupported reducer
			// in an older module) is not a connection error and must not tear
			// down the connection; tests that exercise such reducers handle the
			// missing row themselves. Surface it as a log only.
			t.Logf("reducer error: %v", err)
		})

	for _, opt := range opts {
		builder = opt(builder)
	}

	c, err := builder.Build(ctx)
	require.NoError(t, err)
	conn = c

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx)
	}()

	select {
	case <-connected:
	case err := <-runDone:
		cancel()
		require.NoError(t, err, "Run() exited before OnConnect")
	case <-time.After(defaultTimeout):
		cancel()
		require.Fail(t, "timed out waiting for OnConnect")
	}

	t.Cleanup(func() {
		_ = conn.Disconnect()
		cancel()
		<-runDone
	})

	return conn
}

// waitFor waits for a channel signal or times out.
func waitFor(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		require.Fail(t, "timed out waiting for signal")
	}
}

// waitForCount waits until a table cache reaches the expected row count.
func waitForCount(t *testing.T, tc cache.TableCache, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if tc.Count() >= expected {
			return
		}
		select {
		case <-deadline:
			require.Equal(t, expected, tc.Count(), "timed out waiting for row count")
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForInsert registers an OnInsert callback and waits for it to fire.
func waitForInsert(t *testing.T, tc cache.TableCache, timeout time.Duration) any {
	t.Helper()
	inserted := make(chan any, 1)
	tc.OnInsert(func(row any) {
		select {
		case inserted <- row:
		default:
		}
	})
	select {
	case row := <-inserted:
		return row
	case <-time.After(timeout):
		require.Fail(t, "timed out waiting for insert callback")
		return nil
	}
}

// registerAllTables registers all schema table definitions with the connection.
func registerAllTables(conn client.DbConnection) {
	for _, td := range schema.AllTableDefs() {
		conn.RegisterTable(td)
	}
	for _, td := range schema.AllTableDefsWithPK() {
		conn.RegisterTable(td)
	}
}

// moduleHandle returns the ModuleHandle for the given variant, or skips the test.
func moduleHandle(t *testing.T, variant string) harness.ModuleHandle {
	t.Helper()
	h, ok := moduleHandles[variant]
	if !ok {
		t.Skipf("module variant %q not available", variant)
	}
	return h
}

// skipIfNoModule skips the test if the given module variant is not published.
func skipIfNoModule(t *testing.T, variant string) {
	t.Helper()
	if _, ok := moduleHandles[variant]; !ok {
		t.Skipf("module variant %q not available", variant)
	}
}

// defaultVariant returns the first available module variant, defaulting to "rust".
func defaultVariant() string {
	for variant := range moduleHandles {
		return variant
	}
	return "rust"
}

// defaultDBName returns the database name for the default module variant.
func defaultDBName(t *testing.T) string {
	t.Helper()
	v := defaultVariant()
	h := moduleHandle(t, v)
	return h.DatabaseName()
}

// allVariants returns all available module variant names.
func allVariants() []string {
	variants := make([]string, 0, len(moduleHandles))
	for v := range moduleHandles {
		variants = append(variants, v)
	}
	return variants
}

// subscribe subscribes to the given queries and waits for OnApplied.
func subscribe(t *testing.T, conn client.DbConnection, queries ...string) {
	t.Helper()
	applied := make(chan struct{}, 1)
	_, err := conn.Subscribe(queries...).
		OnApplied(func() {
			select {
			case applied <- struct{}{}:
			default:
			}
		}).
		Build()
	require.NoError(t, err)
	waitFor(t, applied, defaultTimeout)
}
