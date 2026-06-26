package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
)

// fakeWS is an in-memory ws.Connection used to drive the connection event loop
// in tests. Sent frames are published on sent; Recv blocks until a frame is
// pushed on recv or the context is cancelled.
type fakeWS struct {
	sent chan []byte
	recv chan []byte
}

func newFakeWS() *fakeWS {
	return &fakeWS{
		sent: make(chan []byte, 16),
		recv: make(chan []byte),
	}
}

func (f *fakeWS) Send(_ context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	f.sent <- cp
	return nil
}

func (f *fakeWS) Recv(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case d := <-f.recv:
		return d, nil
	}
}

func (f *fakeWS) Close() error { return nil }

// newTestConnection builds a dbConnection wired to a fake websocket, mirroring
// what dbConnectionBuilder.Build constructs but without a real network dial.
func newTestConnection(ws *fakeWS) *dbConnection {
	return &dbConnection{
		ws:          ws,
		cache:       cache.NewClientCache(),
		commands:    make(chan *command, 256),
		incoming:    make(chan []byte, 64),
		compression: protocol.CompressionNone,
	}
}

// nextSent reads the next sent frame or fails the test on timeout.
func nextSent(t *testing.T, f *fakeWS) []byte {
	t.Helper()
	select {
	case b := <-f.sent:
		return b
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a sent frame")
		return nil
	}
}

// decodedClientMsg is a minimal decode of the client->server frames this test
// cares about (SubscribeMulti tag 4, UnsubscribeMulti tag 6).
type decodedClientMsg struct {
	tag       uint8
	queries   []string
	requestID uint32
	queryID   uint32
}

func decodeClientMsg(t *testing.T, data []byte) decodedClientMsg {
	t.Helper()
	r := bsatn.NewReader(data)
	tag, err := r.GetSumTag()
	require.NoError(t, err)
	out := decodedClientMsg{tag: tag}
	switch tag {
	case 4: // SubscribeMulti { query_strings, request_id, query_id }
		n, err := r.GetArrayLen()
		require.NoError(t, err)
		for range n {
			s, err := r.GetString()
			require.NoError(t, err)
			out.queries = append(out.queries, s)
		}
		out.requestID, err = r.GetU32()
		require.NoError(t, err)
		out.queryID, err = r.GetU32()
		require.NoError(t, err)
	case 6: // UnsubscribeMulti { request_id, query_id }
		var err error
		out.requestID, err = r.GetU32()
		require.NoError(t, err)
		out.queryID, err = r.GetU32()
		require.NoError(t, err)
	default:
		t.Fatalf("unexpected client message tag %d", tag)
	}
	return out
}

// TestSubscribeIsAdditive proves the core fix: each Build sends an independent
// SubscribeMulti (tag 4) with a distinct query_id, rather than a v1 Subscribe
// that would replace the entire subscription set.
func TestSubscribeIsAdditive(t *testing.T) {
	fake := newFakeWS()
	conn := newTestConnection(fake)

	go func() { _ = conn.Run(t.Context()) }()

	h1, err := conn.Subscribe("SELECT * FROM a").Build()
	require.NoError(t, err)
	msg1 := decodeClientMsg(t, nextSent(t, fake))

	h2, err := conn.Subscribe("SELECT * FROM b").Build()
	require.NoError(t, err)
	msg2 := decodeClientMsg(t, nextSent(t, fake))

	assert.Equal(t, uint8(4), msg1.tag, "first subscribe must be SubscribeMulti (tag 4)")
	assert.Equal(t, uint8(4), msg2.tag, "second subscribe must be SubscribeMulti (tag 4)")
	assert.Equal(t, []string{"SELECT * FROM a"}, msg1.queries)
	assert.Equal(t, []string{"SELECT * FROM b"}, msg2.queries)

	// The two subscriptions are independent: distinct query ids on the wire and
	// on the handles.
	assert.NotEqual(t, msg1.queryID, msg2.queryID)
	assert.Equal(t, h1.QueryID(), msg1.queryID)
	assert.Equal(t, h2.QueryID(), msg2.queryID)
	assert.NotEqual(t, h1.QueryID(), h2.QueryID())
	assert.True(t, h1.IsActive())
	assert.True(t, h2.IsActive())
}

// TestUnsubscribeSendsUnsubscribeMulti verifies a functional Unsubscribe: it
// emits UnsubscribeMulti (tag 6) for the handle's own query id only.
func TestUnsubscribeSendsUnsubscribeMulti(t *testing.T) {
	fake := newFakeWS()
	conn := newTestConnection(fake)

	go func() { _ = conn.Run(t.Context()) }()

	h, err := conn.Subscribe("SELECT * FROM a").Build()
	require.NoError(t, err)
	subMsg := decodeClientMsg(t, nextSent(t, fake))
	require.Equal(t, uint8(4), subMsg.tag)

	require.NoError(t, h.Unsubscribe())
	unsubMsg := decodeClientMsg(t, nextSent(t, fake))

	assert.Equal(t, uint8(6), unsubMsg.tag, "unsubscribe must be UnsubscribeMulti (tag 6)")
	assert.Equal(t, h.QueryID(), unsubMsg.queryID)
	assert.False(t, h.IsActive())
}

// newDispatchConnection builds a connection with the Run-goroutine state maps
// initialised, for directly exercising dispatchServerMessage without a loop.
func newDispatchConnection() *dbConnection {
	c := &dbConnection{cache: cache.NewClientCache()}
	c.subscriptions = make(map[uint32]*activeSubscription)
	c.requestToQuery = make(map[uint32]uint32)
	c.oneOffCallbacks = make(map[uint64]chan any)
	return c
}

// TestDispatchSubscribeMultiApplied verifies the applied response fires
// onApplied for the matching query id and marks the subscription applied.
func TestDispatchSubscribeMultiApplied(t *testing.T) {
	c := newDispatchConnection()

	applied := false
	c.subscriptions[7] = &activeSubscription{queryID: 7, onApplied: func() { applied = true }}
	c.requestToQuery[1] = 7

	c.dispatchServerMessage(&protocol.SubscribeMultiApplied{RequestID: 1, QueryID: 7})

	assert.True(t, applied, "onApplied should fire")
	require.Contains(t, c.subscriptions, uint32(7))
	assert.True(t, c.subscriptions[7].applied)
	assert.NotContains(t, c.requestToQuery, uint32(1), "request mapping should be cleaned up")
}

// TestDispatchUnsubscribeMultiApplied verifies the subscription is removed from
// the registry once the server confirms the unsubscribe.
func TestDispatchUnsubscribeMultiApplied(t *testing.T) {
	c := newDispatchConnection()
	c.subscriptions[9] = &activeSubscription{queryID: 9}
	c.requestToQuery[2] = 9

	c.dispatchServerMessage(&protocol.UnsubscribeMultiApplied{RequestID: 2, QueryID: 9})

	assert.NotContains(t, c.subscriptions, uint32(9), "subscription should be removed")
	assert.NotContains(t, c.requestToQuery, uint32(2))
}

// TestDispatchSubscriptionError verifies onError fires and the failed
// subscription is dropped, whether matched by query id or request id.
func TestDispatchSubscriptionError(t *testing.T) {
	t.Run("by query id", func(t *testing.T) {
		c := newDispatchConnection()
		var gotErr error
		c.subscriptions[8] = &activeSubscription{queryID: 8, onError: func(e error) { gotErr = e }}
		qid := uint32(8)

		c.dispatchServerMessage(&protocol.V1SubscriptionError{QueryID: &qid, Error: "boom"})

		require.Error(t, gotErr)
		var subErr *SubscriptionError
		require.ErrorAs(t, gotErr, &subErr)
		assert.Equal(t, uint32(8), subErr.QuerySetID)
		assert.Equal(t, "boom", subErr.Message)
		assert.NotContains(t, c.subscriptions, uint32(8))
	})

	t.Run("by request id", func(t *testing.T) {
		c := newDispatchConnection()
		var gotErr error
		c.subscriptions[5] = &activeSubscription{queryID: 5, onError: func(e error) { gotErr = e }}
		c.requestToQuery[3] = 5
		reqID := uint32(3)

		c.dispatchServerMessage(&protocol.V1SubscriptionError{RequestID: &reqID, Error: "nope"})

		require.Error(t, gotErr)
		assert.NotContains(t, c.subscriptions, uint32(5))
		assert.NotContains(t, c.requestToQuery, uint32(3))
	})
}

// TestResubscribeAll verifies the reconnect-replay helper re-sends a
// SubscribeMulti for every tracked subscription using its existing query id.
func TestResubscribeAll(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	c.subscriptions = make(map[uint32]*activeSubscription)
	c.requestToQuery = make(map[uint32]uint32)
	c.oneOffCallbacks = make(map[uint64]chan any)
	c.subscriptions[1] = &activeSubscription{queryID: 1, queries: []string{"SELECT * FROM a"}, applied: true}
	c.subscriptions[2] = &activeSubscription{queryID: 2, queries: []string{"SELECT * FROM b"}, applied: true}

	require.NoError(t, c.resubscribeAll(context.Background()))

	seen := map[uint32]bool{}
	for range 2 {
		m := decodeClientMsg(t, nextSent(t, fake))
		assert.Equal(t, uint8(4), m.tag)
		seen[m.queryID] = true
	}
	assert.Equal(t, map[uint32]bool{1: true, 2: true}, seen)
	// Replayed subscriptions are reset to not-applied until the server confirms.
	assert.False(t, c.subscriptions[1].applied)
	assert.False(t, c.subscriptions[2].applied)
}
