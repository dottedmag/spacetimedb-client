package protocol_test

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
)

// writeRowList writes a FixedSize BsatnRowList to w: every row must have the
// same length rowSize. Used to build server-message wire bytes in tests.
func writeRowList(w bsatn.Writer, rowSize uint16, rows [][]byte) {
	w.PutSumTag(0) // FixedSize
	w.PutU16(rowSize)
	var data []byte
	for _, r := range rows {
		data = append(data, r...)
	}
	w.PutArrayLen(uint32(len(data)))
	w.PutBytes(data)
}

// --- v2 ClientMessage encoding tests ---

func TestSubscribe_WriteBsatn(t *testing.T) {
	msg := &protocol.Subscribe{
		RequestID:    7,
		QuerySetID:   3,
		QueryStrings: []string{"SELECT * FROM users", "SELECT * FROM items"},
	}

	w := bsatn.NewWriter(128)
	msg.WriteBsatn(w)

	exp := bsatn.NewWriter(128)
	exp.PutSumTag(0)
	exp.PutU32(7)
	exp.PutU32(3)
	exp.PutArrayLen(2)
	exp.PutString("SELECT * FROM users")
	exp.PutString("SELECT * FROM items")

	assert.Equal(t, exp.Bytes(), w.Bytes())
}

func TestUnsubscribe_WriteBsatn(t *testing.T) {
	msg := &protocol.Unsubscribe{
		RequestID:  9,
		QuerySetID: 4,
		Flags:      protocol.UnsubscribeSendDroppedRows,
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)

	exp := bsatn.NewWriter(64)
	exp.PutSumTag(1)
	exp.PutU32(9)
	exp.PutU32(4)
	exp.PutU8(1)

	assert.Equal(t, exp.Bytes(), w.Bytes())
}

func TestOneOffQuery_WriteBsatn(t *testing.T) {
	msg := &protocol.OneOffQuery{
		RequestID:   12,
		QueryString: "SELECT count(*) FROM users",
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)

	exp := bsatn.NewWriter(64)
	exp.PutSumTag(2)
	exp.PutU32(12)
	exp.PutString("SELECT count(*) FROM users")

	assert.Equal(t, exp.Bytes(), w.Bytes())
}

func TestCallReducer_WriteBsatn(t *testing.T) {
	msg := &protocol.CallReducer{
		RequestID: 42,
		Flags:     0,
		Reducer:   "add_user",
		Args:      []byte{0xDE, 0xAD},
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)

	// v2 CallReducer: tag=3, request_id(u32), flags(u8), reducer(String), args(Bytes)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(3)
	exp.PutU32(42)
	exp.PutU8(0)
	exp.PutString("add_user")
	bsatn.WriteByteArray(exp, []byte{0xDE, 0xAD})

	assert.Equal(t, exp.Bytes(), w.Bytes())
	assert.Equal(t, uint8(3), w.Bytes()[0], "sum tag should be 3 for CallReducer")
}

func TestCallProcedure_WriteBsatn(t *testing.T) {
	msg := &protocol.CallProcedure{
		RequestID: 55,
		Flags:     1,
		Procedure: "my_proc",
		Args:      []byte{0x01, 0x02, 0x03},
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)

	// v2 CallProcedure: tag=4, request_id(u32), flags(u8), procedure(String), args(Bytes)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(4)
	exp.PutU32(55)
	exp.PutU8(1)
	exp.PutString("my_proc")
	bsatn.WriteByteArray(exp, []byte{0x01, 0x02, 0x03})

	assert.Equal(t, exp.Bytes(), w.Bytes())
}

func TestBsatnEncode_AllClientMessages(t *testing.T) {
	msgs := []protocol.ClientMessage{
		&protocol.Subscribe{RequestID: 1, QuerySetID: 1, QueryStrings: []string{"a"}},
		&protocol.Unsubscribe{RequestID: 2, QuerySetID: 1},
		&protocol.OneOffQuery{RequestID: 3, QueryString: "b"},
		&protocol.CallReducer{RequestID: 4, Reducer: "r", Args: []byte{0x01}},
		&protocol.CallProcedure{RequestID: 5, Procedure: "p", Args: []byte{0x02}},
	}
	wantTags := []uint8{0, 1, 2, 3, 4}
	for i, m := range msgs {
		data := bsatn.Encode(m)
		require.NotEmpty(t, data)
		assert.Equal(t, wantTags[i], data[0], "unexpected sum tag for message %d", i)
	}
}

// --- Compression tests (protocol-version agnostic) ---

func TestDecompressMessage_None(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	data := append([]byte{0x00}, payload...)

	result, err := protocol.DecompressMessage(data)
	require.NoError(t, err)
	assert.Equal(t, payload, result)
}

func TestDecompressMessage_Gzip(t *testing.T) {
	original := []byte("hello world from spacetimedb")

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(original)
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	data := append([]byte{0x02}, buf.Bytes()...)

	result, err := protocol.DecompressMessage(data)
	require.NoError(t, err)
	assert.Equal(t, original, result)
}

func TestDecompressMessage_EmptyPayload(t *testing.T) {
	_, err := protocol.DecompressMessage(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty message")
}

func TestDecompressMessage_UnknownTag(t *testing.T) {
	data := []byte{0xFF, 0x01, 0x02}
	_, err := protocol.DecompressMessage(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown compression tag")
}

func TestDecompressMessage_Brotli(t *testing.T) {
	original := []byte("brotli compressed data for spacetimedb protocol")

	var buf bytes.Buffer
	bw := brotli.NewWriterLevel(&buf, brotli.DefaultCompression)
	_, err := bw.Write(original)
	require.NoError(t, err)
	require.NoError(t, bw.Close())

	data := append([]byte{0x01}, buf.Bytes()...)

	result, err := protocol.DecompressMessage(data)
	require.NoError(t, err)
	assert.Equal(t, original, result)
}

func TestDecompressMessage_None_SingleByte(t *testing.T) {
	data := []byte{0x00}
	result, err := protocol.DecompressMessage(data)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// --- BsatnRowList decoding tests (protocol-version agnostic) ---

func TestBsatnRowList_FixedSize(t *testing.T) {
	w := bsatn.NewWriter(64)
	w.PutSumTag(0)
	w.PutU16(4)
	w.PutArrayLen(8)
	w.PutBytes([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})

	r := bsatn.NewReader(w.Bytes())
	rl, err := protocol.ReadBsatnRowList(r)
	require.NoError(t, err)

	hint, ok := rl.SizeHint.(protocol.FixedSizeHint)
	require.True(t, ok, "expected FixedSizeHint")
	assert.Equal(t, uint16(4), hint.RowSize)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, rl.RowsData)

	rows := rl.Rows()
	require.Len(t, rows, 2)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, rows[0])
	assert.Equal(t, []byte{0x05, 0x06, 0x07, 0x08}, rows[1])

	assert.Equal(t, 2, rl.Len())
}

func TestBsatnRowList_RowOffsets(t *testing.T) {
	w := bsatn.NewWriter(64)
	w.PutSumTag(1)
	w.PutArrayLen(3)
	w.PutU64(0)
	w.PutU64(2)
	w.PutU64(5)
	rowData := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11}
	w.PutArrayLen(uint32(len(rowData)))
	w.PutBytes(rowData)

	r := bsatn.NewReader(w.Bytes())
	rl, err := protocol.ReadBsatnRowList(r)
	require.NoError(t, err)

	hint, ok := rl.SizeHint.(protocol.RowOffsetsHint)
	require.True(t, ok, "expected RowOffsetsHint")
	assert.Equal(t, []uint64{0, 2, 5}, hint.Offsets)

	rows := rl.Rows()
	require.Len(t, rows, 3)
	assert.Equal(t, []byte{0xAA, 0xBB}, rows[0])
	assert.Equal(t, []byte{0xCC, 0xDD, 0xEE}, rows[1])
	assert.Equal(t, []byte{0xFF, 0x11}, rows[2])

	assert.Equal(t, 3, rl.Len())
}

func TestBsatnRowList_EmptyRowsData(t *testing.T) {
	w := bsatn.NewWriter(32)
	w.PutSumTag(0)
	w.PutU16(4)
	w.PutArrayLen(0)

	r := bsatn.NewReader(w.Bytes())
	rl, err := protocol.ReadBsatnRowList(r)
	require.NoError(t, err)
	assert.Equal(t, 0, rl.Len())
	assert.Nil(t, rl.Rows())
}

func TestBsatnRowList_InvalidTag(t *testing.T) {
	w := bsatn.NewWriter(8)
	w.PutSumTag(99)

	r := bsatn.NewReader(w.Bytes())
	_, err := protocol.ReadBsatnRowList(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tag 99")
}

// --- v2 ServerMessage decoding tests ---

func TestReadServerMessage_InitialConnection(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(0)

	var identity [32]byte
	for i := range identity {
		identity[i] = byte(i)
	}
	w.PutBytes(identity[:])

	var connID [16]byte
	for i := range connID {
		connID[i] = byte(0xA0 + i)
	}
	w.PutBytes(connID[:])

	w.PutString("auth-token-xyz")

	msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
	require.NoError(t, err)

	ic, ok := msg.(*protocol.InitialConnection)
	require.True(t, ok, "expected *InitialConnection")
	assert.Equal(t, identity, ic.Identity.Bytes())
	assert.Equal(t, connID, ic.ConnectionID.Bytes())
	assert.Equal(t, "auth-token-xyz", ic.Token)
}

func TestReadServerMessage_SubscribeApplied(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(1)
	w.PutU32(11)         // request_id
	w.PutU32(5)          // query_set_id
	w.PutArrayLen(1)     // QueryRows.tables
	w.PutString("users") // SingleTableRows.table
	writeRowList(w, 4, [][]byte{{0x01, 0x02, 0x03, 0x04}})

	msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
	require.NoError(t, err)

	sa, ok := msg.(*protocol.SubscribeApplied)
	require.True(t, ok, "expected *SubscribeApplied")
	assert.Equal(t, uint32(11), sa.RequestID)
	assert.Equal(t, uint32(5), sa.QuerySetID)
	require.Len(t, sa.Rows.Tables, 1)
	assert.Equal(t, "users", sa.Rows.Tables[0].TableName)
	require.Len(t, sa.Rows.Tables[0].Rows.Rows(), 1)
}

func TestReadServerMessage_UnsubscribeApplied_NoRows(t *testing.T) {
	w := bsatn.NewWriter(64)
	w.PutSumTag(2)
	w.PutU32(1)    // request_id
	w.PutU32(2)    // query_set_id
	w.PutSumTag(1) // Option<QueryRows> = None

	msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
	require.NoError(t, err)

	ua, ok := msg.(*protocol.UnsubscribeApplied)
	require.True(t, ok, "expected *UnsubscribeApplied")
	assert.Equal(t, uint32(2), ua.QuerySetID)
	assert.Nil(t, ua.Rows)
}

func TestReadServerMessage_SubscriptionError(t *testing.T) {
	t.Run("with request id", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(3)
		w.PutSumTag(0) // Option<u32> = Some
		w.PutU32(77)
		w.PutU32(8) // query_set_id
		w.PutString("bad query")

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		se, ok := msg.(*protocol.SubscriptionError)
		require.True(t, ok)
		require.NotNil(t, se.RequestID)
		assert.Equal(t, uint32(77), *se.RequestID)
		assert.Equal(t, uint32(8), se.QuerySetID)
		assert.Equal(t, "bad query", se.Error)
	})

	t.Run("without request id", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(3)
		w.PutSumTag(1) // Option<u32> = None
		w.PutU32(8)
		w.PutString("late failure")

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		se := msg.(*protocol.SubscriptionError)
		assert.Nil(t, se.RequestID)
		assert.Equal(t, "late failure", se.Error)
	})
}

func TestReadServerMessage_TransactionUpdate_PersistentAndEvent(t *testing.T) {
	w := bsatn.NewWriter(256)
	w.PutSumTag(4)
	w.PutArrayLen(1) // query_sets
	w.PutU32(5)      // query_set_id
	w.PutArrayLen(2) // tables

	// table 0: persistent (insert + delete)
	w.PutString("users")
	w.PutArrayLen(1)                                       // rows
	w.PutSumTag(0)                                         // PersistentTable
	writeRowList(w, 4, [][]byte{{0x0A, 0x0B, 0x0C, 0x0D}}) // inserts
	writeRowList(w, 4, [][]byte{{0x01, 0x02, 0x03, 0x04}}) // deletes

	// table 1: event
	w.PutString("pings")
	w.PutArrayLen(1)                                         // rows
	w.PutSumTag(1)                                           // EventTable
	writeRowList(w, 2, [][]byte{{0xFE, 0xFF}, {0xAB, 0xCD}}) // events

	msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
	require.NoError(t, err)

	tu, ok := msg.(*protocol.TransactionUpdate)
	require.True(t, ok, "expected *TransactionUpdate")
	require.Len(t, tu.QuerySets, 1)
	qs := tu.QuerySets[0]
	assert.Equal(t, uint32(5), qs.QuerySetID)
	require.Len(t, qs.Tables, 2)

	require.Len(t, qs.Tables[0].Rows, 1)
	pt, ok := qs.Tables[0].Rows[0].(*protocol.PersistentTableRows)
	require.True(t, ok, "expected *PersistentTableRows")
	require.Len(t, pt.Inserts.Rows(), 1)
	assert.Equal(t, []byte{0x0A, 0x0B, 0x0C, 0x0D}, pt.Inserts.Rows()[0])
	require.Len(t, pt.Deletes.Rows(), 1)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, pt.Deletes.Rows()[0])

	require.Len(t, qs.Tables[1].Rows, 1)
	et, ok := qs.Tables[1].Rows[0].(*protocol.EventTableRows)
	require.True(t, ok, "expected *EventTableRows")
	require.Len(t, et.Events.Rows(), 2)
}

func TestReadServerMessage_OneOffQueryResult(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		w := bsatn.NewWriter(128)
		w.PutSumTag(5)
		w.PutU32(3)      // request_id
		w.PutSumTag(0)   // Ok
		w.PutArrayLen(1) // QueryRows.tables
		w.PutString("users")
		writeRowList(w, 4, [][]byte{{0x09, 0x09, 0x09, 0x09}})

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		res := msg.(*protocol.OneOffQueryResult)
		assert.Equal(t, uint32(3), res.RequestID)
		require.NotNil(t, res.Rows)
		assert.Nil(t, res.Error)
		require.Len(t, res.Rows.Tables, 1)
	})

	t.Run("err", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(5)
		w.PutU32(3)
		w.PutSumTag(1) // Err
		w.PutString("syntax error")

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		res := msg.(*protocol.OneOffQueryResult)
		require.NotNil(t, res.Error)
		assert.Equal(t, "syntax error", *res.Error)
		assert.Nil(t, res.Rows)
	})
}

func TestReadServerMessage_ReducerResult(t *testing.T) {
	t.Run("ok with transaction update", func(t *testing.T) {
		w := bsatn.NewWriter(128)
		w.PutSumTag(6)
		w.PutU32(2)                           // request_id
		w.PutI64(1700000000000000)            // timestamp
		w.PutSumTag(0)                        // ReducerOutcome::Ok
		bsatn.WriteByteArray(w, []byte{0x42}) // ret_value
		w.PutArrayLen(1)                      // transaction_update.query_sets
		w.PutU32(5)                           // query_set_id
		w.PutArrayLen(1)                      // tables
		w.PutString("users")
		w.PutArrayLen(1)
		w.PutSumTag(0) // PersistentTable
		writeRowList(w, 4, [][]byte{{0x11, 0x22, 0x33, 0x44}})
		writeRowList(w, 4, nil)

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		rr := msg.(*protocol.ReducerResult)
		assert.Equal(t, uint32(2), rr.RequestID)
		ok, isOk := rr.Outcome.(*protocol.ReducerOk)
		require.True(t, isOk, "expected *ReducerOk")
		assert.Equal(t, []byte{0x42}, ok.RetValue)
		require.Len(t, ok.TransactionUpdate.QuerySets, 1)
	})

	t.Run("ok empty", func(t *testing.T) {
		w := bsatn.NewWriter(32)
		w.PutSumTag(6)
		w.PutU32(2)
		w.PutI64(0)
		w.PutSumTag(1) // OkEmpty
		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		_, ok := msg.(*protocol.ReducerResult).Outcome.(*protocol.ReducerOkEmpty)
		assert.True(t, ok, "expected *ReducerOkEmpty")
	})

	t.Run("internal error", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(6)
		w.PutU32(2)
		w.PutI64(0)
		w.PutSumTag(3) // InternalError
		w.PutString("reducer panicked")
		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		ie, ok := msg.(*protocol.ReducerResult).Outcome.(*protocol.ReducerInternalError)
		require.True(t, ok, "expected *ReducerInternalError")
		assert.Equal(t, "reducer panicked", ie.Message)
	})
}

func TestReadServerMessage_ProcedureResult(t *testing.T) {
	t.Run("returned", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(7)
		w.PutSumTag(0) // Returned
		bsatn.WriteByteArray(w, []byte{0x01, 0x02, 0x03})
		w.PutI64(123) // timestamp
		w.PutI64(456) // duration
		w.PutU32(99)  // request_id

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		pr := msg.(*protocol.ProcedureResult)
		assert.Equal(t, uint32(99), pr.RequestID)
		ret, ok := pr.Status.(*protocol.ProcedureReturned)
		require.True(t, ok, "expected *ProcedureReturned")
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, ret.Value)
	})

	t.Run("internal error", func(t *testing.T) {
		w := bsatn.NewWriter(64)
		w.PutSumTag(7)
		w.PutSumTag(1) // InternalError
		w.PutString("no such procedure")
		w.PutI64(0)
		w.PutI64(0)
		w.PutU32(13)

		msg, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.NoError(t, err)
		pr := msg.(*protocol.ProcedureResult)
		ie, ok := pr.Status.(*protocol.ProcedureInternalError)
		require.True(t, ok, "expected *ProcedureInternalError")
		assert.Equal(t, "no such procedure", ie.Message)
	})
}

func TestReadServerMessage_InvalidTag(t *testing.T) {
	w := bsatn.NewWriter(8)
	w.PutSumTag(99)
	_, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
	require.Error(t, err)
}
