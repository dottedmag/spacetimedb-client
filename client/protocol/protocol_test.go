package protocol_test

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
)

// --- ClientMessage encoding tests ---

func TestCallReducer_WriteBsatn(t *testing.T) {
	msg := &protocol.CallReducer{
		RequestID: 42,
		Flags:     0,
		Reducer:   "add_user",
		Args:      []byte{0xDE, 0xAD},
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 CallReducer: tag=0, reducer(String), args(Bytes), request_id(u32), flags(u8)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(0)
	exp.PutString("add_user")
	bsatn.WriteByteArray(exp, []byte{0xDE, 0xAD})
	exp.PutU32(42)
	exp.PutU8(0)
	expected := exp.Bytes()

	assert.Equal(t, expected, got)
	assert.Equal(t, uint8(0), got[0], "sum tag should be 0 for CallReducer")
}

func TestCallReducer_EmptyArgs(t *testing.T) {
	msg := &protocol.CallReducer{
		RequestID: 1,
		Flags:     0,
		Reducer:   "noop",
		Args:      nil,
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	exp := bsatn.NewWriter(64)
	exp.PutSumTag(0)
	exp.PutString("noop")
	bsatn.WriteByteArray(exp, nil)
	exp.PutU32(1)
	exp.PutU8(0)

	assert.Equal(t, exp.Bytes(), got)
}

func TestSubscribe_WriteBsatn(t *testing.T) {
	msg := &protocol.Subscribe{
		RequestID:    10,
		QueryStrings: []string{"SELECT * FROM users", "SELECT * FROM items"},
	}

	w := bsatn.NewWriter(128)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 Subscribe: tag=1, query_strings(Vec<String>), request_id(u32)
	exp := bsatn.NewWriter(128)
	exp.PutSumTag(1)
	exp.PutArrayLen(2)
	exp.PutString("SELECT * FROM users")
	exp.PutString("SELECT * FROM items")
	exp.PutU32(10)

	assert.Equal(t, exp.Bytes(), got)
	assert.Equal(t, uint8(1), got[0], "sum tag should be 1 for Subscribe")
}

func TestSubscribe_EmptyQueries(t *testing.T) {
	msg := &protocol.Subscribe{
		RequestID:    1,
		QueryStrings: nil,
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	exp := bsatn.NewWriter(64)
	exp.PutSumTag(1)
	exp.PutArrayLen(0)
	exp.PutU32(1)

	assert.Equal(t, exp.Bytes(), got)
}

func TestUnsubscribe_WriteBsatn(t *testing.T) {
	msg := &protocol.Unsubscribe{
		RequestID: 7,
		QueryID:   3,
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 Unsubscribe: tag=5, request_id(u32), query_id(u32)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(5)
	exp.PutU32(7)
	exp.PutU32(3)

	assert.Equal(t, exp.Bytes(), got)
}

func TestOneOffQuery_WriteBsatn(t *testing.T) {
	msgID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	msg := &protocol.OneOffQuery{
		MessageID:   msgID,
		QueryString: "SELECT count(*) FROM users",
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 OneOffQuery: tag=2, message_id(Bytes), query_string(String)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(2)
	bsatn.WriteByteArray(exp, msgID)
	exp.PutString("SELECT count(*) FROM users")

	assert.Equal(t, exp.Bytes(), got)
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
	got := w.Bytes()

	// v1 CallProcedure: tag=7, procedure(String), args(Bytes), request_id(u32), flags(u8)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(7)
	exp.PutString("my_proc")
	bsatn.WriteByteArray(exp, []byte{0x01, 0x02, 0x03})
	exp.PutU32(55)
	exp.PutU8(1)

	assert.Equal(t, exp.Bytes(), got)
}

func TestSubscribeMulti_WriteBsatn(t *testing.T) {
	msg := &protocol.SubscribeMulti{
		RequestID:    10,
		QueryID:      5,
		QueryStrings: []string{"SELECT * FROM users"},
	}

	w := bsatn.NewWriter(128)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 SubscribeMulti: tag=4, query_strings(Vec<String>), request_id(u32), query_id(u32)
	exp := bsatn.NewWriter(128)
	exp.PutSumTag(4)
	exp.PutArrayLen(1)
	exp.PutString("SELECT * FROM users")
	exp.PutU32(10)
	exp.PutU32(5)

	assert.Equal(t, exp.Bytes(), got)
}

func TestUnsubscribeMulti_WriteBsatn(t *testing.T) {
	msg := &protocol.UnsubscribeMulti{
		RequestID: 9,
		QueryID:   4,
	}

	w := bsatn.NewWriter(64)
	msg.WriteBsatn(w)
	got := w.Bytes()

	// v1 UnsubscribeMulti: tag=6, request_id(u32), query_id(u32)
	exp := bsatn.NewWriter(64)
	exp.PutSumTag(6)
	exp.PutU32(9)
	exp.PutU32(4)

	assert.Equal(t, exp.Bytes(), got)
}

// --- Compression tests ---

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

// --- BsatnRowList decoding tests ---

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

func TestBsatnRowList_NilRowList(t *testing.T) {
	var rl *protocol.BsatnRowList
	assert.Equal(t, 0, rl.Len())
	assert.Nil(t, rl.Rows())
}

func TestBsatnRowList_ZeroRowSize(t *testing.T) {
	w := bsatn.NewWriter(32)
	w.PutSumTag(0)
	w.PutU16(0)
	w.PutArrayLen(4)
	w.PutBytes([]byte{0x01, 0x02, 0x03, 0x04})

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

// --- ServerMessage decoding tests ---

func TestReadServerMessage_IdentityToken(t *testing.T) {
	// Build BSATN for an IdentityToken message (tag 3)
	w := bsatn.NewWriter(128)
	w.PutSumTag(3) // IdentityToken tag

	// Identity: 32 bytes
	var identityBytes [32]byte
	for i := range identityBytes {
		identityBytes[i] = byte(i)
	}
	w.PutBytes(identityBytes[:])

	// Token: string
	w.PutString("my-auth-token-123")

	// ConnectionId: 16 bytes
	var connIdBytes [16]byte
	for i := range connIdBytes {
		connIdBytes[i] = byte(0xA0 + i)
	}
	w.PutBytes(connIdBytes[:])

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	it, ok := msg.(*protocol.IdentityToken)
	require.True(t, ok, "expected *IdentityToken")
	assert.Equal(t, identityBytes, it.Identity.Bytes())
	assert.Equal(t, "my-auth-token-123", it.Token)
	assert.Equal(t, connIdBytes, it.ConnectionID.Bytes())
}

func TestReadServerMessage_InitialSubscription(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(0) // InitialSubscription tag

	// V1DatabaseUpdate: tables (Vec<V1TableUpdate>) - empty
	w.PutArrayLen(0)

	// request_id: u32
	w.PutU32(42)

	// total_host_execution_duration: TimeDuration (i64 microseconds)
	w.PutI64(5000)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	is, ok := msg.(*protocol.InitialSubscription)
	require.True(t, ok, "expected *InitialSubscription")
	assert.Equal(t, uint32(42), is.RequestID)
	assert.Empty(t, is.DatabaseUpdate.Tables)
}

func TestReadServerMessage_V1TransactionUpdateLight(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(2) // V1TransactionUpdateLight tag

	// request_id: u32
	w.PutU32(100)

	// V1DatabaseUpdate: tables (Vec<V1TableUpdate>) - empty
	w.PutArrayLen(0)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	tul, ok := msg.(*protocol.V1TransactionUpdateLight)
	require.True(t, ok, "expected *V1TransactionUpdateLight")
	assert.Equal(t, uint32(100), tul.RequestID)
	assert.Empty(t, tul.DatabaseUpdate.Tables)
}

func TestReadServerMessage_V1TransactionUpdateLight_WithTables(t *testing.T) {
	w := bsatn.NewWriter(256)
	w.PutSumTag(2) // V1TransactionUpdateLight tag

	// request_id
	w.PutU32(200)

	// V1DatabaseUpdate: 1 table
	w.PutArrayLen(1)
	// V1TableUpdate: table_id, table_name, num_rows, updates
	w.PutU32(1)            // table_id
	w.PutString("players") // table_name
	w.PutU64(10)           // num_rows
	w.PutArrayLen(1)       // 1 CompressableQueryUpdate
	// Uncompressed: tag=0, deletes + inserts
	w.PutSumTag(0)
	// deletes: empty BsatnRowList
	w.PutSumTag(0)
	w.PutU16(4)
	w.PutArrayLen(0)
	// inserts: BsatnRowList with 1 row of 4 bytes
	w.PutSumTag(0)
	w.PutU16(4)
	w.PutArrayLen(4)
	w.PutBytes([]byte{0x0A, 0x0B, 0x0C, 0x0D})

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	tul, ok := msg.(*protocol.V1TransactionUpdateLight)
	require.True(t, ok, "expected *V1TransactionUpdateLight")
	assert.Equal(t, uint32(200), tul.RequestID)
	require.Len(t, tul.DatabaseUpdate.Tables, 1)
	assert.Equal(t, "players", tul.DatabaseUpdate.Tables[0].TableName)
	assert.Equal(t, uint32(1), tul.DatabaseUpdate.Tables[0].TableID)
	require.Len(t, tul.DatabaseUpdate.Tables[0].Updates, 1)

	uncompressed, ok := tul.DatabaseUpdate.Tables[0].Updates[0].(*protocol.V1Uncompressed)
	require.True(t, ok, "expected *V1Uncompressed")
	assert.Equal(t, 1, uncompressed.Inserts.Len())
	assert.Equal(t, 0, uncompressed.Deletes.Len())
}

func TestReadServerMessage_V1SubscriptionError(t *testing.T) {
	w := bsatn.NewWriter(64)
	w.PutSumTag(7) // V1SubscriptionError tag

	// duration: u64
	w.PutU64(5000)

	// request_id: Option<u32> = Some(77)
	w.PutSumTag(0)
	w.PutU32(77)

	// query_id: Option<u32> = None
	w.PutSumTag(1)

	// table_id: Option<u32> = None
	w.PutSumTag(1)

	// error: String
	w.PutString("table not found")

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	se, ok := msg.(*protocol.V1SubscriptionError)
	require.True(t, ok, "expected *V1SubscriptionError")
	require.NotNil(t, se.RequestID)
	assert.Equal(t, uint32(77), *se.RequestID)
	assert.Nil(t, se.QueryID)
	assert.Nil(t, se.TableID)
	assert.Equal(t, "table not found", se.Error)
}

func TestReadServerMessage_V1SubscriptionError_NoRequestID(t *testing.T) {
	w := bsatn.NewWriter(64)
	w.PutSumTag(7) // V1SubscriptionError tag

	// duration: u64
	w.PutU64(1000)

	// request_id: Option<u32> = None
	w.PutSumTag(1)

	// query_id: Option<u32> = None
	w.PutSumTag(1)

	// table_id: Option<u32> = None
	w.PutSumTag(1)

	// error: String
	w.PutString("unknown error")

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	se, ok := msg.(*protocol.V1SubscriptionError)
	require.True(t, ok, "expected *V1SubscriptionError")
	assert.Nil(t, se.RequestID)
	assert.Equal(t, "unknown error", se.Error)
}

func TestReadServerMessage_OneOffQueryResponse_WithError(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(4) // OneOffQueryResponse tag

	// message_id: Bytes
	msgID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	bsatn.WriteByteArray(w, msgID)

	// error: Option<String> = Some("syntax error")
	w.PutSumTag(0)
	w.PutString("syntax error near SELECT")

	// tables: Vec<OneOffTable> - empty
	w.PutArrayLen(0)

	// duration: TimeDuration (i64)
	w.PutI64(100)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	oqr, ok := msg.(*protocol.OneOffQueryResponse)
	require.True(t, ok, "expected *OneOffQueryResponse")
	assert.Equal(t, msgID, oqr.MessageID)
	require.NotNil(t, oqr.Error)
	assert.Equal(t, "syntax error near SELECT", *oqr.Error)
}

func TestReadServerMessage_OneOffQueryResponse_Ok(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(4) // OneOffQueryResponse tag

	// message_id: Bytes
	msgID := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	bsatn.WriteByteArray(w, msgID)

	// error: Option<String> = None
	w.PutSumTag(1)

	// tables: Vec<OneOffTable> - 1 table
	w.PutArrayLen(1)
	w.PutString("counts")
	// BsatnRowList: FixedSize(4), 4 bytes
	w.PutSumTag(0)
	w.PutU16(4)
	w.PutArrayLen(4)
	w.PutBytes([]byte{0x01, 0x00, 0x00, 0x00})

	// duration: TimeDuration (i64)
	w.PutI64(200)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	oqr, ok := msg.(*protocol.OneOffQueryResponse)
	require.True(t, ok, "expected *OneOffQueryResponse")
	assert.Equal(t, msgID, oqr.MessageID)
	assert.Nil(t, oqr.Error)
	require.Len(t, oqr.Tables, 1)
	assert.Equal(t, "counts", oqr.Tables[0].TableName)
	assert.Equal(t, 1, oqr.Tables[0].Rows.Len())
}

func TestReadServerMessage_SubscribeMultiApplied(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(8) // SubscribeMultiApplied tag

	// request_id: u32
	w.PutU32(10)

	// duration: u64
	w.PutU64(3000)

	// query_id: u32
	w.PutU32(5)

	// V1DatabaseUpdate: empty tables
	w.PutArrayLen(0)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	sma, ok := msg.(*protocol.SubscribeMultiApplied)
	require.True(t, ok, "expected *SubscribeMultiApplied")
	assert.Equal(t, uint32(10), sma.RequestID)
	assert.Equal(t, uint64(3000), sma.Duration)
	assert.Equal(t, uint32(5), sma.QueryID)
	assert.Empty(t, sma.Update.Tables)
}

func TestReadServerMessage_UnsubscribeMultiApplied(t *testing.T) {
	w := bsatn.NewWriter(128)
	w.PutSumTag(9) // UnsubscribeMultiApplied tag

	// request_id: u32
	w.PutU32(20)

	// duration: u64
	w.PutU64(4000)

	// query_id: u32
	w.PutU32(7)

	// V1DatabaseUpdate: empty tables
	w.PutArrayLen(0)

	r := bsatn.NewReader(w.Bytes())
	msg, err := protocol.ReadServerMessage(r)
	require.NoError(t, err)

	uma, ok := msg.(*protocol.UnsubscribeMultiApplied)
	require.True(t, ok, "expected *UnsubscribeMultiApplied")
	assert.Equal(t, uint32(20), uma.RequestID)
	assert.Equal(t, uint64(4000), uma.Duration)
	assert.Equal(t, uint32(7), uma.QueryID)
}

func TestReadServerMessage_InvalidTag(t *testing.T) {
	w := bsatn.NewWriter(8)
	w.PutSumTag(99)

	r := bsatn.NewReader(w.Bytes())
	_, err := protocol.ReadServerMessage(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tag 99")
}

// --- V1 CompressableQueryUpdate decompression tests ---

func TestDecompressV1QueryUpdate_Uncompressed(t *testing.T) {
	// Build an uncompressed query update directly
	update := &protocol.V1Uncompressed{
		Deletes: &protocol.BsatnRowList{
			SizeHint: protocol.FixedSizeHint{RowSize: 4},
			RowsData: nil,
		},
		Inserts: &protocol.BsatnRowList{
			SizeHint: protocol.FixedSizeHint{RowSize: 4},
			RowsData: []byte{0x01, 0x02, 0x03, 0x04},
		},
	}

	result, err := protocol.DecompressV1QueryUpdate(update)
	require.NoError(t, err)
	assert.Equal(t, update, result)
}

func TestDecompressV1QueryUpdate_Brotli(t *testing.T) {
	// Build a QueryUpdate {deletes, inserts} in BSATN
	inner := bsatn.NewWriter(64)
	// deletes: empty BsatnRowList
	inner.PutSumTag(0)
	inner.PutU16(4)
	inner.PutArrayLen(0)
	// inserts: BsatnRowList with 1 row
	inner.PutSumTag(0)
	inner.PutU16(4)
	inner.PutArrayLen(4)
	inner.PutBytes([]byte{0xAA, 0xBB, 0xCC, 0xDD})

	// Brotli compress
	var buf bytes.Buffer
	bw := brotli.NewWriterLevel(&buf, brotli.DefaultCompression)
	_, err := bw.Write(inner.Bytes())
	require.NoError(t, err)
	require.NoError(t, bw.Close())

	compressed := &protocol.V1CompressedBrotli{Data: buf.Bytes()}
	result, err := protocol.DecompressV1QueryUpdate(compressed)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.Deletes.Len())
	assert.Equal(t, 1, result.Inserts.Len())
}

func TestDecompressV1QueryUpdate_Gzip(t *testing.T) {
	// Build a QueryUpdate {deletes, inserts} in BSATN
	inner := bsatn.NewWriter(64)
	// deletes: empty BsatnRowList
	inner.PutSumTag(0)
	inner.PutU16(2)
	inner.PutArrayLen(0)
	// inserts: BsatnRowList with 2 rows of size 2
	inner.PutSumTag(0)
	inner.PutU16(2)
	inner.PutArrayLen(4)
	inner.PutBytes([]byte{0x01, 0x02, 0x03, 0x04})

	// Gzip compress
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(inner.Bytes())
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	compressed := &protocol.V1CompressedGzip{Data: buf.Bytes()}
	result, err := protocol.DecompressV1QueryUpdate(compressed)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.Deletes.Len())
	assert.Equal(t, 2, result.Inserts.Len())
}

// --- Edge case: large messages ---

func TestSubscribe_ManyQueries(t *testing.T) {
	queries := make([]string, 100)
	for i := range queries {
		queries[i] = "SELECT * FROM table_" + string(rune('A'+i%26))
	}

	msg := &protocol.Subscribe{
		RequestID:    1,
		QueryStrings: queries,
	}

	w := bsatn.NewWriter(4096)
	msg.WriteBsatn(w)
	got := w.Bytes()

	require.NotEmpty(t, got)
	assert.Equal(t, uint8(1), got[0], "tag should be 1")

	// Verify array length is encoded as 100
	// After tag byte: array_len(u32)
	arrayLenOffset := 1 // tag
	assert.Equal(t, uint32(100), binary.LittleEndian.Uint32(got[arrayLenOffset:arrayLenOffset+4]))
}

func TestCallReducer_LargeArgs(t *testing.T) {
	largeArgs := make([]byte, 65536)
	for i := range largeArgs {
		largeArgs[i] = byte(i % 256)
	}

	msg := &protocol.CallReducer{
		RequestID: 1,
		Flags:     0,
		Reducer:   "bulk_insert",
		Args:      largeArgs,
	}

	w := bsatn.NewWriter(70000)
	msg.WriteBsatn(w)
	got := w.Bytes()
	require.NotEmpty(t, got)

	// tag(1) + string_len(4) + "bulk_insert"(11) + array_len(4) + 65536 + request_id(4) + flags(1)
	expectedMinSize := 1 + 4 + 11 + 4 + 65536 + 4 + 1
	assert.GreaterOrEqual(t, len(got), expectedMinSize)
}

func TestBsatnRowList_ManyRowsFixedSize(t *testing.T) {
	rowSize := uint16(8)
	numRows := 1000
	rowData := make([]byte, int(rowSize)*numRows)
	for i := range rowData {
		rowData[i] = byte(i % 256)
	}

	w := bsatn.NewWriter(len(rowData) + 16)
	w.PutSumTag(0)
	w.PutU16(rowSize)
	w.PutArrayLen(uint32(len(rowData)))
	w.PutBytes(rowData)

	r := bsatn.NewReader(w.Bytes())
	rl, err := protocol.ReadBsatnRowList(r)
	require.NoError(t, err)
	assert.Equal(t, numRows, rl.Len())

	rows := rl.Rows()
	require.Len(t, rows, numRows)
	for i, row := range rows {
		require.Len(t, row, int(rowSize), "row %d should have size %d", i, rowSize)
	}
}

// --- BSATN encode helper round-trip ---

func TestBsatnEncode_CallReducer(t *testing.T) {
	msg := &protocol.CallReducer{
		RequestID: 1,
		Flags:     0,
		Reducer:   "test",
		Args:      []byte{0x01},
	}

	encoded := bsatn.Encode(msg)
	require.NotEmpty(t, encoded)

	// Verify the tag byte (v1 CallReducer = tag 0)
	assert.Equal(t, uint8(0), encoded[0])
}

func TestBsatnEncode_AllClientMessages(t *testing.T) {
	messages := []protocol.ClientMessage{
		&protocol.CallReducer{RequestID: 4, Flags: 0, Reducer: "test", Args: nil},
		&protocol.Subscribe{RequestID: 1, QueryStrings: []string{"SELECT 1"}},
		&protocol.OneOffQuery{MessageID: []byte{0x01}, QueryString: "SELECT 1"},
		&protocol.SubscribeMulti{RequestID: 1, QueryID: 1, QueryStrings: []string{"SELECT 1"}},
		&protocol.Unsubscribe{RequestID: 2, QueryID: 1},
		&protocol.UnsubscribeMulti{RequestID: 3, QueryID: 2},
		&protocol.CallProcedure{RequestID: 5, Flags: 0, Procedure: "test", Args: nil},
	}

	expectedTags := []uint8{0, 1, 2, 4, 5, 6, 7}

	for i, msg := range messages {
		encoded := bsatn.Encode(msg)
		require.NotEmpty(t, encoded, "message %d should encode", i)
		assert.Equal(t, expectedTags[i], encoded[0], "message %d should have tag %d", i, expectedTags[i])
	}
}
