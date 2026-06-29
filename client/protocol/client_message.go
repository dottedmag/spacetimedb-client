package protocol

import "go.digitalxero.dev/spacetimedb-client/bsatn"

// ClientMessage represents a message sent from client to server.
// It is a BSATN sum type with variants for each message kind.
//
// v2 ClientMessage enum (v2.bsatn.spacetimedb):
//
//	Tag 0: Subscribe       { request_id, query_set_id, query_strings }
//	Tag 1: Unsubscribe     { request_id, query_set_id, flags }
//	Tag 2: OneOffQuery     { request_id, query_string }
//	Tag 3: CallReducer     { request_id, flags, reducer, args }
//	Tag 4: CallProcedure   { request_id, flags, procedure, args }
//
// Note: a v2 QuerySetId is a single-field product { id: u32 } and so encodes
// as a bare u32 on the wire.
type ClientMessage interface {
	bsatn.Serializable
	clientMessageTag() uint8
}

// Subscribe registers a set of SQL queries as a query set identified by
// QuerySetID. Tag 0 in the v2 ClientMessage sum type.
type Subscribe struct {
	RequestID    uint32
	QuerySetID   uint32
	QueryStrings []string
}

func (*Subscribe) clientMessageTag() uint8 { return 0 }

func (s *Subscribe) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(0)
	w.PutU32(s.RequestID)
	w.PutU32(s.QuerySetID)
	w.PutArrayLen(uint32(len(s.QueryStrings)))
	for _, q := range s.QueryStrings {
		w.PutString(q)
	}
}

// UnsubscribeFlags is the v2 Unsubscribe flag enum (u8).
// Default = 0; SendDroppedRows = 1 requests the server return the removed rows.
type UnsubscribeFlags uint8

const (
	UnsubscribeDefault         UnsubscribeFlags = 0
	UnsubscribeSendDroppedRows UnsubscribeFlags = 1
)

// Unsubscribe ends a previously-registered query set. Tag 1 in the v2
// ClientMessage sum type.
type Unsubscribe struct {
	RequestID  uint32
	QuerySetID uint32
	Flags      UnsubscribeFlags
}

func (*Unsubscribe) clientMessageTag() uint8 { return 1 }

func (u *Unsubscribe) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(1)
	w.PutU32(u.RequestID)
	w.PutU32(u.QuerySetID)
	w.PutU8(uint8(u.Flags))
}

// OneOffQuery runs a query once without subscribing to updates.
// Tag 2 in the v2 ClientMessage sum type.
type OneOffQuery struct {
	RequestID   uint32
	QueryString string
}

func (*OneOffQuery) clientMessageTag() uint8 { return 2 }

func (o *OneOffQuery) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(2)
	w.PutU32(o.RequestID)
	w.PutString(o.QueryString)
}

// CallReducer invokes a reducer (transactional database function).
// Tag 3 in the v2 ClientMessage sum type.
type CallReducer struct {
	RequestID uint32
	Flags     uint8
	Reducer   string
	Args      []byte
}

func (*CallReducer) clientMessageTag() uint8 { return 3 }

func (c *CallReducer) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(3)
	w.PutU32(c.RequestID)
	w.PutU8(c.Flags)
	w.PutString(c.Reducer)
	bsatn.WriteByteArray(w, c.Args)
}

// CallProcedure invokes a procedure (non-transactional database function).
// Tag 4 in the v2 ClientMessage sum type.
type CallProcedure struct {
	RequestID uint32
	Flags     uint8
	Procedure string
	Args      []byte
}

func (*CallProcedure) clientMessageTag() uint8 { return 4 }

func (c *CallProcedure) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(4)
	w.PutU32(c.RequestID)
	w.PutU8(c.Flags)
	w.PutString(c.Procedure)
	bsatn.WriteByteArray(w, c.Args)
}
