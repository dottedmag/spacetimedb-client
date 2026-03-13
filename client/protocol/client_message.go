package protocol

import "go.digitalxero.dev/spacetimedb-client/bsatn"

// ClientMessage represents a message sent from client to server.
// It is a BSATN sum type with variants for each message kind.
//
// v1 ClientMessage enum:
//   Tag 0: CallReducer { reducer, args, request_id, flags }
//   Tag 1: Subscribe { query_strings, request_id }
//   Tag 2: OneOffQuery { message_id, query_string }
//   Tag 3: SubscribeSingle { query, request_id, query_id }
//   Tag 4: SubscribeMulti { query_strings, request_id, query_id }
//   Tag 5: Unsubscribe { request_id, query_id }
//   Tag 6: UnsubscribeMulti { request_id, query_id }
//   Tag 7: CallProcedure { procedure, args, request_id, flags }
type ClientMessage interface {
	bsatn.Serializable
	clientMessageTag() uint8
}

// CallReducer invokes a reducer (transactional database function).
// Tag 0 in the v1 ClientMessage sum type.
// Fields: reducer(String), args(Bytes), request_id(u32), flags(u8).
type CallReducer struct {
	Reducer   string
	Args      []byte
	RequestID uint32
	Flags     uint8
}

func (*CallReducer) clientMessageTag() uint8 { return 0 }

func (c *CallReducer) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(0)
	w.PutString(c.Reducer)
	bsatn.WriteByteArray(w, c.Args)
	w.PutU32(c.RequestID)
	w.PutU8(c.Flags)
}

// Subscribe registers a set of SQL queries replacing the entire current subscription.
// Tag 1 in the v1 ClientMessage sum type.
// Fields: query_strings(Vec<String>), request_id(u32).
type Subscribe struct {
	QueryStrings []string
	RequestID    uint32
}

func (*Subscribe) clientMessageTag() uint8 { return 1 }

func (s *Subscribe) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(1)
	w.PutArrayLen(uint32(len(s.QueryStrings)))
	for _, q := range s.QueryStrings {
		w.PutString(q)
	}
	w.PutU32(s.RequestID)
}

// OneOffQuery runs a query once without subscribing to updates.
// Tag 2 in the v1 ClientMessage sum type.
// Fields: message_id(Bytes), query_string(String).
type OneOffQuery struct {
	MessageID   []byte
	QueryString string
}

func (*OneOffQuery) clientMessageTag() uint8 { return 2 }

func (o *OneOffQuery) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(2)
	bsatn.WriteByteArray(w, o.MessageID)
	w.PutString(o.QueryString)
}

// SubscribeMulti subscribes to a set of queries with a query_id (v1 multi-query subscription).
// Tag 4 in the v1 ClientMessage sum type.
// Fields: query_strings(Vec<String>), request_id(u32), query_id(u32).
type SubscribeMulti struct {
	QueryStrings []string
	RequestID    uint32
	QueryID      uint32
}

func (*SubscribeMulti) clientMessageTag() uint8 { return 4 }

func (s *SubscribeMulti) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(4)
	w.PutArrayLen(uint32(len(s.QueryStrings)))
	for _, q := range s.QueryStrings {
		w.PutString(q)
	}
	w.PutU32(s.RequestID)
	w.PutU32(s.QueryID)
}

// Unsubscribe removes a previously-registered subscription.
// Tag 5 in the v1 ClientMessage sum type.
// Fields: request_id(u32), query_id(u32).
type Unsubscribe struct {
	RequestID uint32
	QueryID   uint32
}

func (*Unsubscribe) clientMessageTag() uint8 { return 5 }

func (u *Unsubscribe) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(5)
	w.PutU32(u.RequestID)
	w.PutU32(u.QueryID)
}

// UnsubscribeMulti removes a multi subscription.
// Tag 6 in the v1 ClientMessage sum type.
// Fields: request_id(u32), query_id(u32).
type UnsubscribeMulti struct {
	RequestID uint32
	QueryID   uint32
}

func (*UnsubscribeMulti) clientMessageTag() uint8 { return 6 }

func (u *UnsubscribeMulti) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(6)
	w.PutU32(u.RequestID)
	w.PutU32(u.QueryID)
}

// CallProcedure invokes a procedure (non-transactional database function).
// Tag 7 in the v1 ClientMessage sum type.
type CallProcedure struct {
	Procedure string
	Args      []byte
	RequestID uint32
	Flags     uint8
}

func (*CallProcedure) clientMessageTag() uint8 { return 7 }

func (c *CallProcedure) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(7)
	w.PutString(c.Procedure)
	bsatn.WriteByteArray(w, c.Args)
	w.PutU32(c.RequestID)
	w.PutU8(c.Flags)
}
