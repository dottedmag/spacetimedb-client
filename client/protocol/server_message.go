package protocol

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// ServerMessage represents a message sent from the server to the client.
// v2 ServerMessage<BsatnFormat> sum type (v2.bsatn.spacetimedb):
//
//	Tag 0: InitialConnection  { identity, connection_id, token }
//	Tag 1: SubscribeApplied   { request_id, query_set_id, rows }
//	Tag 2: UnsubscribeApplied { request_id, query_set_id, rows: Option<QueryRows> }
//	Tag 3: SubscriptionError  { request_id: Option<u32>, query_set_id, error }
//	Tag 4: TransactionUpdate  { query_sets }
//	Tag 5: OneOffQueryResult  { request_id, result: Result<QueryRows, String> }
//	Tag 6: ReducerResult      { request_id, timestamp, result: ReducerOutcome }
//	Tag 7: ProcedureResult    { status, timestamp, total_host_execution_duration, request_id }
type ServerMessage interface {
	serverMessageTag() uint8
}

// InitialConnection is the first message sent upon a successful connection (tag 0).
// It carries the client's identity, connection id, and auth token.
type InitialConnection struct {
	Identity     types.Identity
	ConnectionID types.ConnectionId
	Token        string
}

func (*InitialConnection) serverMessageTag() uint8 { return 0 }

// SubscribeApplied is sent in response to a Subscribe message (tag 1), carrying
// the initial matching rows for the query set.
type SubscribeApplied struct {
	RequestID  uint32
	QuerySetID uint32
	Rows       QueryRows
}

func (*SubscribeApplied) serverMessageTag() uint8 { return 1 }

// UnsubscribeApplied confirms removal of a query set (tag 2). Rows is non-nil
// only when the Unsubscribe requested dropped rows via SendDroppedRows.
type UnsubscribeApplied struct {
	RequestID  uint32
	QuerySetID uint32
	Rows       *QueryRows
}

func (*UnsubscribeApplied) serverMessageTag() uint8 { return 2 }

// SubscriptionError notifies the client of a subscription failure (tag 3).
// RequestID is nil when the failure occurred while computing a TransactionUpdate
// rather than during initial application.
type SubscriptionError struct {
	RequestID  *uint32
	QuerySetID uint32
	Error      string
}

func (*SubscriptionError) serverMessageTag() uint8 { return 3 }

// TransactionUpdate broadcasts row changes to the client's subscribed query sets
// after a committed transaction (tag 4).
type TransactionUpdate struct {
	QuerySets []QuerySetUpdate
}

func (*TransactionUpdate) serverMessageTag() uint8 { return 4 }

// OneOffQueryResult is sent in response to a OneOffQuery (tag 5). Exactly one of
// Rows (on success) or Error (on failure) is non-nil.
type OneOffQueryResult struct {
	RequestID uint32
	Rows      *QueryRows
	Error     *string
}

func (*OneOffQueryResult) serverMessageTag() uint8 { return 5 }

// ReducerResult is sent in response to a CallReducer message (tag 6), carrying
// the reducer's exit status and, when committed, its transaction update.
type ReducerResult struct {
	RequestID uint32
	Timestamp types.Timestamp
	Outcome   ReducerOutcome
}

func (*ReducerResult) serverMessageTag() uint8 { return 6 }

// ReducerOutcome is the v2 sum type for a reducer run result.
// Tag 0 = Ok(ReducerOk), Tag 1 = OkEmpty, Tag 2 = Err(Bytes), Tag 3 = InternalError(String).
type ReducerOutcome interface {
	isReducerOutcome()
}

// ReducerOk means the reducer committed; it carries the BSATN return value and
// the transaction update for the caller's query sets.
type ReducerOk struct {
	RetValue          []byte
	TransactionUpdate TransactionUpdate
}

func (*ReducerOk) isReducerOutcome() {}

// ReducerOkEmpty means the reducer committed with a zero-byte return value and an
// empty transaction update (a wire-size optimization).
type ReducerOkEmpty struct{}

func (*ReducerOkEmpty) isReducerOutcome() {}

// ReducerErr means the reducer returned a structured error and did not commit.
// Value is the BSATN encoding of the reducer's error return type.
type ReducerErr struct {
	Value []byte
}

func (*ReducerErr) isReducerOutcome() {}

// ReducerInternalError means the reducer panicked or failed due to a host error.
type ReducerInternalError struct {
	Message string
}

func (*ReducerInternalError) isReducerOutcome() {}

// ProcedureResult is sent in response to a CallProcedure message (tag 7).
type ProcedureResult struct {
	Status                     ProcedureStatus
	Timestamp                  types.Timestamp
	TotalHostExecutionDuration types.TimeDuration
	RequestID                  uint32
}

func (*ProcedureResult) serverMessageTag() uint8 { return 7 }

// ProcedureStatus is the v2 sum type for a procedure run result.
// Tag 0 = Returned(Bytes), Tag 1 = InternalError(String). (v2 has no OutOfEnergy variant.)
type ProcedureStatus interface {
	isProcedureStatus()
}

// ProcedureReturned means the procedure ran and returned the enclosed value.
// Value is the BSATN encoding of the procedure's declared return type.
type ProcedureReturned struct {
	Value []byte
}

func (*ProcedureReturned) isProcedureStatus() {}

// ProcedureInternalError means the call failed in the host, e.g. a type error or
// unknown procedure name.
type ProcedureInternalError struct {
	Message string
}

func (*ProcedureInternalError) isProcedureStatus() {}

// ReadServerMessage reads a v2 ServerMessage from a BSATN reader by dispatching on the sum type tag.
func ReadServerMessage(r bsatn.Reader) (ServerMessage, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}

	switch tag {
	case 0:
		return readInitialConnection(r)
	case 1:
		return readSubscribeApplied(r)
	case 2:
		return readUnsubscribeApplied(r)
	case 3:
		return readSubscriptionError(r)
	case 4:
		return readTransactionUpdate(r)
	case 5:
		return readOneOffQueryResult(r)
	case 6:
		return readReducerResult(r)
	case 7:
		return readProcedureResult(r)
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "ServerMessage"}
	}
}
