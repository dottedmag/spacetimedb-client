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

// decodedClientMsg is a minimal decode of the v2 client->server frames this test
// cares about (Subscribe tag 0, Unsubscribe tag 1, CallProcedure tag 4).
type decodedClientMsg struct {
	tag        uint8
	queries    []string
	requestID  uint32
	querySetID uint32
	procedure  string
	args       []byte
}

func decodeClientMsg(t *testing.T, data []byte) decodedClientMsg {
	t.Helper()
	r := bsatn.NewReader(data)
	tag, err := r.GetSumTag()
	require.NoError(t, err)
	out := decodedClientMsg{tag: tag}
	switch tag {
	case 0: // Subscribe { request_id, query_set_id, query_strings }
		out.requestID, err = r.GetU32()
		require.NoError(t, err)
		out.querySetID, err = r.GetU32()
		require.NoError(t, err)
		n, err := r.GetArrayLen()
		require.NoError(t, err)
		for range n {
			s, err := r.GetString()
			require.NoError(t, err)
			out.queries = append(out.queries, s)
		}
	case 1: // Unsubscribe { request_id, query_set_id, flags }
		out.requestID, err = r.GetU32()
		require.NoError(t, err)
		out.querySetID, err = r.GetU32()
		require.NoError(t, err)
		_, err = r.GetU8()
		require.NoError(t, err)
	case 4: // CallProcedure { request_id, flags, procedure, args }
		out.requestID, err = r.GetU32()
		require.NoError(t, err)
		_, err = r.GetU8() // flags
		require.NoError(t, err)
		out.procedure, err = r.GetString()
		require.NoError(t, err)
		out.args, err = bsatn.ReadByteArray(r)
		require.NoError(t, err)
	default:
		t.Fatalf("unexpected client message tag %d", tag)
	}
	return out
}

// TestSubscribeIsAdditive proves each Build sends an independent v2 Subscribe
// (tag 0) with a distinct query_set_id.
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

	assert.Equal(t, uint8(0), msg1.tag, "first subscribe must be v2 Subscribe (tag 0)")
	assert.Equal(t, uint8(0), msg2.tag, "second subscribe must be v2 Subscribe (tag 0)")
	assert.Equal(t, []string{"SELECT * FROM a"}, msg1.queries)
	assert.Equal(t, []string{"SELECT * FROM b"}, msg2.queries)

	assert.NotEqual(t, msg1.querySetID, msg2.querySetID)
	assert.Equal(t, h1.QueryID(), msg1.querySetID)
	assert.Equal(t, h2.QueryID(), msg2.querySetID)
}

// TestUnsubscribeSendsUnsubscribe verifies Unsubscribe emits a v2 Unsubscribe
// (tag 1) for the handle's own query_set_id only.
func TestUnsubscribeSendsUnsubscribe(t *testing.T) {
	fake := newFakeWS()
	conn := newTestConnection(fake)

	go func() { _ = conn.Run(t.Context()) }()

	h, err := conn.Subscribe("SELECT * FROM a").Build()
	require.NoError(t, err)
	subMsg := decodeClientMsg(t, nextSent(t, fake))
	require.Equal(t, uint8(0), subMsg.tag)

	require.NoError(t, h.Unsubscribe())
	unsubMsg := decodeClientMsg(t, nextSent(t, fake))

	assert.Equal(t, uint8(1), unsubMsg.tag, "unsubscribe must be v2 Unsubscribe (tag 1)")
	assert.Equal(t, h.QueryID(), unsubMsg.querySetID)
	assert.False(t, h.IsActive())
}

// newDispatchConnection builds a connection with the Run-goroutine state maps
// initialised, for directly exercising dispatchServerMessage without a loop.
func newDispatchConnection() *dbConnection {
	c := &dbConnection{cache: cache.NewClientCache()}
	c.subscriptions = make(map[uint32]*activeSubscription)
	c.oneOffCallbacks = make(map[uint32]chan any)
	c.procedureCallbacks = make(map[uint32]chan any)
	c.reducerNames = make(map[uint32]string)
	return c
}

// TestDispatchSubscribeApplied verifies the applied response fires onApplied for
// the matching query_set_id and marks the subscription applied.
func TestDispatchSubscribeApplied(t *testing.T) {
	c := newDispatchConnection()

	applied := false
	c.subscriptions[7] = &activeSubscription{queryID: 7, onApplied: func() { applied = true }}

	c.dispatchServerMessage(&protocol.SubscribeApplied{RequestID: 1, QuerySetID: 7})

	assert.True(t, applied, "onApplied should fire")
	require.Contains(t, c.subscriptions, uint32(7))
	assert.True(t, c.subscriptions[7].applied)
}

// TestDispatchUnsubscribeApplied verifies the subscription is removed once the
// server confirms the unsubscribe.
func TestDispatchUnsubscribeApplied(t *testing.T) {
	c := newDispatchConnection()
	c.subscriptions[9] = &activeSubscription{queryID: 9}

	c.dispatchServerMessage(&protocol.UnsubscribeApplied{RequestID: 2, QuerySetID: 9})

	assert.NotContains(t, c.subscriptions, uint32(9), "subscription should be removed")
}

// TestDispatchSubscriptionError verifies onError fires and the failed
// subscription is dropped, matched by query_set_id.
func TestDispatchSubscriptionError(t *testing.T) {
	c := newDispatchConnection()
	var gotErr error
	c.subscriptions[8] = &activeSubscription{queryID: 8, onError: func(e error) { gotErr = e }}

	c.dispatchServerMessage(&protocol.SubscriptionError{QuerySetID: 8, Error: "boom"})

	require.Error(t, gotErr)
	var subErr *SubscriptionError
	require.ErrorAs(t, gotErr, &subErr)
	assert.Equal(t, uint32(8), subErr.QuerySetID)
	assert.Equal(t, "boom", subErr.Message)
	assert.NotContains(t, c.subscriptions, uint32(8))
}

// TestDispatchReducerResult_Error verifies a failed reducer surfaces via
// onReducerError (with the tracked reducer name) and never via onConnectError,
// and that the request_id -> name entry is cleaned up.
func TestDispatchReducerResult_Error(t *testing.T) {
	c := newDispatchConnection()
	var reducerErr error
	var connErr error
	c.onReducerError = func(e error) { reducerErr = e }
	c.onConnectError = func(e error) { connErr = e }
	c.reducerNames[3] = "insert_thing"

	c.dispatchServerMessage(&protocol.ReducerResult{
		RequestID: 3,
		Outcome:   &protocol.ReducerInternalError{Message: "no such reducer"},
	})

	require.Error(t, reducerErr)
	assert.NoError(t, connErr, "reducer failure must not hit onConnectError")
	var re *ReducerError
	require.ErrorAs(t, reducerErr, &re)
	assert.Equal(t, "insert_thing", re.ReducerName)
	assert.Equal(t, "no such reducer", re.Message)
	assert.NotContains(t, c.reducerNames, uint32(3), "name mapping should be cleaned up")
}

// TestDispatchReducerResult_Ok verifies a committed reducer applies its embedded
// transaction update to the cache and does not fire onReducerError.
func TestDispatchReducerResult_Ok(t *testing.T) {
	c := newDispatchConnection()
	c.cache.RegisterTable(u32TableDef{name: "users"})
	c.reducerNames[4] = "insert_user"

	var fired bool
	c.onReducerError = func(error) { fired = true }
	var inserted []uint32
	c.cache.GetTable("users").OnInsert(func(row any) { inserted = append(inserted, row.(uint32)) })

	c.dispatchServerMessage(&protocol.ReducerResult{
		RequestID: 4,
		Outcome: &protocol.ReducerOk{
			RetValue: nil,
			TransactionUpdate: protocol.TransactionUpdate{
				QuerySets: []protocol.QuerySetUpdate{
					{
						QuerySetID: 1,
						Tables: []protocol.TableUpdate{
							{
								TableName: "users",
								Rows: []protocol.TableUpdateRows{
									&protocol.PersistentTableRows{Inserts: rowListU32(42), Deletes: rowListU32()},
								},
							},
						},
					},
				},
			},
		},
	})

	assert.False(t, fired, "onReducerError must not fire on success")
	assert.Equal(t, []uint32{42}, inserted)
	assert.NotContains(t, c.reducerNames, uint32(4))
}

// --- transaction update apply (persistent + event) ---

// u32TableDef is a minimal cache.TableDef whose rows are a single u32, used to
// drive cache application from dispatchServerMessage in tests.
type u32TableDef struct{ name string }

func (d u32TableDef) TableName() string { return d.name }
func (d u32TableDef) DecodeRow(r bsatn.Reader) (any, error) {
	v, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	return v, nil
}
func (d u32TableDef) EncodeRow(row any) []byte {
	w := bsatn.NewWriter(4)
	w.PutU32(row.(uint32))
	return w.Bytes()
}

// rowListU32 builds a fixed-size (4-byte) BsatnRowList from u32 row values.
func rowListU32(vals ...uint32) *protocol.BsatnRowList {
	var data []byte
	for _, v := range vals {
		w := bsatn.NewWriter(4)
		w.PutU32(v)
		data = append(data, w.Bytes()...)
	}
	return &protocol.BsatnRowList{
		SizeHint: protocol.FixedSizeHint{RowSize: 4},
		RowsData: data,
	}
}

// TestDispatchTransactionUpdate_PersistentAndEvent verifies a TransactionUpdate
// applies persistent inserts to the cache (firing OnInsert) and delivers event
// rows via OnEvent without retaining them.
func TestDispatchTransactionUpdate_PersistentAndEvent(t *testing.T) {
	c := newDispatchConnection()
	c.cache.RegisterTable(u32TableDef{name: "users"})
	c.cache.RegisterTable(u32TableDef{name: "pings"})

	var inserted []uint32
	c.cache.GetTable("users").OnInsert(func(row any) { inserted = append(inserted, row.(uint32)) })

	var events []uint32
	c.cache.GetTable("pings").OnEvent(func(row any) { events = append(events, row.(uint32)) })

	c.dispatchServerMessage(&protocol.TransactionUpdate{
		QuerySets: []protocol.QuerySetUpdate{
			{
				QuerySetID: 1,
				Tables: []protocol.TableUpdate{
					{
						TableName: "users",
						Rows: []protocol.TableUpdateRows{
							&protocol.PersistentTableRows{Inserts: rowListU32(100, 101), Deletes: rowListU32()},
						},
					},
					{
						TableName: "pings",
						Rows: []protocol.TableUpdateRows{
							&protocol.EventTableRows{Events: rowListU32(7, 8, 9)},
						},
					},
				},
			},
		},
	})

	assert.ElementsMatch(t, []uint32{100, 101}, inserted)
	assert.Equal(t, []uint32{7, 8, 9}, events)

	// Persistent rows are retained; event rows are not.
	assert.Equal(t, 2, c.cache.GetTable("users").Count())
	assert.Equal(t, 0, c.cache.GetTable("pings").Count(), "event rows must not be retained")
}

// rawArgs is a trivial bsatn.Serializable that writes a fixed byte payload, used
// to verify a procedure's encoded args reach the wire intact.
type rawArgs struct{ data []byte }

func (a rawArgs) WriteBsatn(w bsatn.Writer) { w.PutBytes(a.data) }

// procedureReturnedFrame hand-encodes a v2 ProcedureResult (tag 7) with a
// Returned status carrying value, wrapped in the uncompressed wire framing the
// read loop expects (a leading CompressionNone tag byte).
func procedureReturnedFrame(requestID uint32, value []byte) []byte {
	w := bsatn.NewWriter(64)
	w.PutSumTag(7)                 // ServerMessage::ProcedureResult
	w.PutSumTag(0)                 // ProcedureStatus::Returned
	bsatn.WriteByteArray(w, value) // return value bytes
	w.PutI64(1)                    // timestamp
	w.PutI64(2)                    // total_host_execution_duration
	w.PutU32(requestID)            // request_id
	body := w.Bytes()
	frame := make([]byte, 0, len(body)+1)
	frame = append(frame, byte(protocol.CompressionNone))
	return append(frame, body...)
}

// TestCallProcedureRoundTrip drives a full CallProcedure: it sends a
// CallProcedure (tag 4) frame, then resolves when a matching ProcedureResult
// (tag 7) carrying a Returned value arrives, returning the raw return bytes.
func TestCallProcedureRoundTrip(t *testing.T) {
	fake := newFakeWS()
	conn := newTestConnection(fake)

	go func() { _ = conn.Run(t.Context()) }()

	type callResult struct {
		value []byte
		err   error
	}
	done := make(chan callResult, 1)
	go func() {
		v, err := conn.CallProcedure(t.Context(), "get_user", rawArgs{data: []byte{0xAA, 0xBB}})
		done <- callResult{v, err}
	}()

	sent := decodeClientMsg(t, nextSent(t, fake))
	require.Equal(t, uint8(4), sent.tag, "must send CallProcedure (tag 4)")
	assert.Equal(t, "get_user", sent.procedure)
	assert.Equal(t, []byte{0xAA, 0xBB}, sent.args)

	fake.recv <- procedureReturnedFrame(sent.requestID, []byte{0x01, 0x02, 0x03})

	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, got.value)
	case <-time.After(2 * time.Second):
		t.Fatal("CallProcedure did not return after ProcedureResult")
	}
}

// TestDispatchProcedureResult verifies each v2 ProcedureStatus variant is
// delivered to the waiting caller's channel and the callback entry is cleaned up.
func TestDispatchProcedureResult(t *testing.T) {
	t.Run("returned", func(t *testing.T) {
		c := newDispatchConnection()
		ch := make(chan any, 1)
		c.procedureCallbacks[5] = ch

		c.dispatchServerMessage(&protocol.ProcedureResult{
			Status:    &protocol.ProcedureReturned{Value: []byte{0x09}},
			RequestID: 5,
		})

		r := (<-ch).(procedureResult)
		require.NoError(t, r.err)
		assert.Equal(t, []byte{0x09}, r.value)
		assert.NotContains(t, c.procedureCallbacks, uint32(5), "callback should be cleaned up")
	})

	t.Run("internal error", func(t *testing.T) {
		c := newDispatchConnection()
		ch := make(chan any, 1)
		c.procedureCallbacks[8] = ch

		c.dispatchServerMessage(&protocol.ProcedureResult{
			Status:    &protocol.ProcedureInternalError{Message: "boom"},
			RequestID: 8,
		})

		r := (<-ch).(procedureResult)
		var pe *ProcedureError
		require.ErrorAs(t, r.err, &pe)
		assert.Equal(t, "boom", pe.Message)
	})
}

// TestCallProcedureContextCancelled verifies the caller unblocks with the
// context error when no result arrives before the context is cancelled.
func TestCallProcedureContextCancelled(t *testing.T) {
	fake := newFakeWS()
	conn := newTestConnection(fake)

	go func() { _ = conn.Run(t.Context()) }()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := conn.CallProcedure(ctx, "slow_proc", nil)
		done <- err
	}()

	sent := decodeClientMsg(t, nextSent(t, fake))
	require.Equal(t, uint8(4), sent.tag)

	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("CallProcedure did not unblock on context cancellation")
	}
}

// TestResubscribeAll verifies the reconnect-replay helper re-sends a v2 Subscribe
// for every tracked subscription using its existing query_set_id.
func TestResubscribeAll(t *testing.T) {
	fake := newFakeWS()
	c := newTestConnection(fake)
	c.subscriptions = make(map[uint32]*activeSubscription)
	c.oneOffCallbacks = make(map[uint32]chan any)
	c.subscriptions[1] = &activeSubscription{queryID: 1, queries: []string{"SELECT * FROM a"}, applied: true}
	c.subscriptions[2] = &activeSubscription{queryID: 2, queries: []string{"SELECT * FROM b"}, applied: true}

	require.NoError(t, c.resubscribeAll(context.Background()))

	seen := map[uint32]bool{}
	for range 2 {
		m := decodeClientMsg(t, nextSent(t, fake))
		assert.Equal(t, uint8(0), m.tag)
		seen[m.querySetID] = true
	}
	assert.Equal(t, map[uint32]bool{1: true, 2: true}, seen)
	assert.False(t, c.subscriptions[1].applied)
	assert.False(t, c.subscriptions[2].applied)
}
