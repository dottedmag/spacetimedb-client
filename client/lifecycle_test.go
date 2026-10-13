package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

func TestDisconnectReleasesPendingRequests(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	query := make(chan error, 1)
	procedure := make(chan error, 1)
	go func() { _, err := c.OneOffQuery("SELECT * FROM player"); query <- err }()
	go func() { _, err := c.CallProcedure(context.Background(), "echo", nil); procedure <- err }()
	nextSent(t, fake)
	nextSent(t, fake)
	cancel()
	for _, ch := range []chan error{done, query, procedure} {
		select {
		case err := <-ch:
			require.Error(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("pending operation did not finish")
		}
	}
	require.Error(t, c.CallReducer("echo", nil))
	require.Error(t, c.Run(t.Context()))
	require.NoError(t, c.Disconnect())
}

func TestReaderShutdownWithFullIncomingQueue(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	c.incoming = make(chan []byte, 1)
	c.incoming <- []byte{0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.readLoop(ctx) }()
	fake.recv <- []byte{0}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reader blocked during shutdown")
	}
}

func TestQueueFullDoesNotBlockCallback(t *testing.T) {
	c := newTestConnection(newFakeWS())
	c.commands = make(chan *command, 1)
	require.NoError(t, c.CallReducer("first", nil))
	require.Error(t, c.CallReducer("second", nil))
}

func TestOneOffQueryContextCancellation(t *testing.T) {
	c := newTestConnection(newFakeWS())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.OneOffQueryContext(ctx, "SELECT * FROM player")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, c.commands)
}

func TestV3CoalescedMessages(t *testing.T) {
	c := newDispatchConnection()
	c.protocol = "v3.bsatn.spacetimedb"
	c.oneOffCallbacks[1] = make(chan any, 1)
	c.oneOffCallbacks[2] = make(chan any, 1)
	w := bsatn.NewWriter(64)
	w.PutU8(0) // no compression
	for _, id := range []uint32{1, 2} {
		w.PutSumTag(5)
		w.PutU32(id)
		w.PutSumTag(1)
		w.PutString("query error")
	}
	require.NoError(t, c.handleIncoming(w.Bytes()))
	require.Empty(t, c.oneOffCallbacks)
	c.protocol = "v2.bsatn.spacetimedb"
	require.Error(t, c.handleIncoming(w.Bytes()))
}

func TestMalformedMessageDisconnects(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	done := make(chan error, 1)
	c.onDisconnect = func(_ DbConnection, err error) { var pe *ProtocolError; require.True(t, errors.As(err, &pe)) }
	go func() { done <- c.Run(t.Context()) }()
	fake.recv <- []byte{0, 255}
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("malformed frame did not terminate connection")
	}
}

func TestRowDecodeFailureDoesNotApplySubscription(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	c.RegisterTable(identityKeyDef{})
	applied := false
	done := make(chan error, 1)
	go func() { done <- c.Run(t.Context()) }()
	_, err := c.Subscribe("SELECT * FROM identity_key").OnApplied(func() { applied = true }).Build()
	require.NoError(t, err)
	request := decodeClientMsg(t, nextSent(t, fake))
	w := bsatn.NewWriter(64)
	w.PutU8(0)
	w.PutSumTag(1)
	w.PutU32(request.requestID)
	w.PutU32(request.querySetID)
	w.PutArrayLen(1)
	w.PutString("identity_key")
	w.PutSumTag(0)
	w.PutU16(1)
	w.PutArrayLen(1)
	w.PutU8(255)
	fake.recv <- w.Bytes()
	select {
	case err := <-done:
		var pe *ProtocolError
		require.ErrorAs(t, err, &pe)
	case <-time.After(time.Second):
		t.Fatal("row decode failure did not stop the connection")
	}
	require.False(t, applied)
	require.Zero(t, c.Cache().GetTable("identity_key").Count())
}
