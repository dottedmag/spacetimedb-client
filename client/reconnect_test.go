package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
	"go.digitalxero.dev/spacetimedb-client/client/ws"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func TestAutomaticReconnectRefreshesTokenAndReplaysSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts, tokens atomic.Int32
	errorsCh := make(chan error, 4)
	replayed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if r.Header.Get("Authorization") != "Bearer token" {
			errorsCh <- errors.New("missing provider token")
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"v2.bsatn.spacetimedb"}})
		if err != nil {
			errorsCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		writer := bsatn.NewWriter(80)
		writer.PutSumTag(0)
		types.NewIdentity([32]byte{1}).WriteBsatn(writer)
		types.NewConnectionId([16]byte{byte(attempt)}).WriteBsatn(writer)
		writer.PutString("token")
		frame := append([]byte{0}, writer.Bytes()...)
		if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
			errorsCh <- err
			return
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			errorsCh <- err
			return
		}
		reader := bsatn.NewReader(data)
		tag, err := reader.GetSumTag()
		if err != nil || tag != 0 {
			errorsCh <- errors.New("expected subscribe")
			return
		}
		if attempt == 1 {
			return
		}
		close(replayed)
		_, _, _ = conn.Read(ctx)
	}))
	defer server.Close()
	conn, err := NewDbConnection().WithUri(server.URL).WithDatabaseName("db").WithAutomaticReconnect().WithTokenProvider(func(context.Context) (string, error) { tokens.Add(1); return "token", nil }).Build(ctx)
	require.NoError(t, err)
	_, err = conn.Subscribe("SELECT * FROM users").Build()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- conn.Run(ctx) }()
	select {
	case <-replayed:
	case err := <-errorsCh:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("reconnect did not replay")
	}
	require.GreaterOrEqual(t, tokens.Load(), int32(2))
	require.NoError(t, conn.Disconnect())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Disconnect did not stop Run")
	}
}

func TestReconnectSnapshotPreservesUnchangedRowsAndRemovesMissingTables(t *testing.T) {
	c := newDispatchConnection()
	c.cache.RegisterTable(u32TableDef{name: "users"})
	c.cache.RegisterTable(u32TableDef{name: "gone"})
	sub := &activeSubscription{}
	old := protocol.QueryRows{Tables: []protocol.SingleTableRows{{TableName: "users", Rows: rowListU32(1, 2)}, {TableName: "gone", Rows: rowListU32(3)}}}
	c.applyQueryRows(&old)
	for _, table := range old.Tables {
		c.trackSubscriptionRows(sub, table.TableName, table.Rows, nil)
	}
	var inserted, deleted []uint32
	c.cache.GetTable("users").OnInsert(func(row any) { inserted = append(inserted, row.(uint32)) })
	c.cache.GetTable("users").OnDelete(func(row any) { deleted = append(deleted, row.(uint32)) })
	c.reconcileSubscription(sub, &protocol.QueryRows{Tables: []protocol.SingleTableRows{{TableName: "users", Rows: rowListU32(2, 4)}}})
	require.Nil(t, c.rowDecodeErr)
	require.Equal(t, []uint32{4}, inserted)
	require.Equal(t, []uint32{1}, deleted)
	require.Equal(t, 2, c.cache.GetTable("users").Count())
	require.Zero(t, c.cache.GetTable("gone").Count())
	c.reconcileSubscription(sub, &protocol.QueryRows{Tables: []protocol.SingleTableRows{{TableName: "users", Rows: rowListU32(2, 4)}}})
	require.Equal(t, []uint32{4}, inserted)
}

func TestDisconnectCancelsReconnectBackoff(t *testing.T) {
	c := newTestConnection(newFakeWS())
	c.ws = brokenReconnectSocket{}
	c.automaticReconnect = true
	dialing := make(chan struct{})
	c.dial = func(ctx context.Context, _ string) (ws.Connection, error) {
		close(dialing)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-dialing:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not start")
	}
	require.NoError(t, c.Disconnect())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown timed out")
	}
}

type brokenReconnectSocket struct{}

func (brokenReconnectSocket) Send(context.Context, []byte) error { return errors.New("transport lost") }
func (brokenReconnectSocket) Recv(context.Context) ([]byte, error) {
	return nil, errors.New("transport lost")
}
func (brokenReconnectSocket) Close() error { return nil }
