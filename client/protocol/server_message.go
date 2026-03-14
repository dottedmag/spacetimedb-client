package protocol

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// ServerMessage represents a message sent from the server to the client.
// v1 ServerMessage<BsatnFormat> sum type:
//
//	Tag 0: InitialSubscription { database_update, request_id, total_host_execution_duration }
//	Tag 1: TransactionUpdate { status, timestamp, caller_identity, caller_connection_id, reducer_call, energy_quanta_used, total_host_execution_duration }
//	Tag 2: TransactionUpdateLight { request_id, update }
//	Tag 3: IdentityToken { identity, token, connection_id }
//	Tag 4: OneOffQueryResponse { message_id, error, tables, total_host_execution_duration }
//	Tag 5: SubscribeApplied { request_id, total_host_execution_duration_micros, query_id, rows }
//	Tag 6: UnsubscribeApplied { ... }
//	Tag 7: SubscriptionError { ... }
//	Tag 8: SubscribeMultiApplied { ... }
//	Tag 9: UnsubscribeMultiApplied { ... }
//	Tag 10: ProcedureResult { ... }
type ServerMessage interface {
	serverMessageTag() uint8
}

// IdentityToken is sent upon a successful connection (tag 3 in v1).
type IdentityToken struct {
	Identity     types.Identity
	Token        string
	ConnectionID types.ConnectionId
}

func (*IdentityToken) serverMessageTag() uint8 { return 3 }

// InitialSubscription is sent in response to a Subscribe message (tag 0 in v1).
// Contains the initial matching rows for the entire subscription set.
type InitialSubscription struct {
	DatabaseUpdate V1DatabaseUpdate
	RequestID      uint32
	Duration       types.TimeDuration
}

func (*InitialSubscription) serverMessageTag() uint8 { return 0 }

// V1TransactionUpdate is sent after a reducer run (tag 1 in v1).
// Contains full metadata about the reducer call and its result.
type V1TransactionUpdate struct {
	Status                     V1UpdateStatus
	Timestamp                  types.Timestamp
	CallerIdentity             types.Identity
	CallerConnectionID         types.ConnectionId
	ReducerCall                V1ReducerCallInfo
	EnergyQuantaUsed           types.EnergyQuanta
	TotalHostExecutionDuration types.TimeDuration
}

func (*V1TransactionUpdate) serverMessageTag() uint8 { return 1 }

// V1TransactionUpdateLight is sent after a reducer run with minimal metadata (tag 2 in v1).
type V1TransactionUpdateLight struct {
	RequestID      uint32
	DatabaseUpdate V1DatabaseUpdate
}

func (*V1TransactionUpdateLight) serverMessageTag() uint8 { return 2 }

// V1UpdateStatus is a sum type for the transaction status.
// Tag 0 = Committed(DatabaseUpdate), Tag 1 = Failed(String), Tag 2 = OutOfEnergy.
type V1UpdateStatus interface {
	isV1UpdateStatus()
}

// V1Committed means the reducer succeeded and its changes were committed.
type V1Committed struct {
	DatabaseUpdate V1DatabaseUpdate
}

func (*V1Committed) isV1UpdateStatus() {}

// V1Failed means the reducer errored.
type V1Failed struct {
	Message string
}

func (*V1Failed) isV1UpdateStatus() {}

// V1OutOfEnergy means the reducer ran out of energy.
type V1OutOfEnergy struct{}

func (*V1OutOfEnergy) isV1UpdateStatus() {}

// V1ReducerCallInfo contains metadata about the reducer call.
type V1ReducerCallInfo struct {
	ReducerName string
	ReducerID   uint32
	Args        []byte // BSATN-encoded args
	RequestID   uint32
}

// V1DatabaseUpdate is a collection of table updates.
type V1DatabaseUpdate struct {
	Tables []V1TableUpdate
}

// V1TableUpdate describes row changes for a single table.
type V1TableUpdate struct {
	TableID   uint32
	TableName string
	NumRows   uint64
	Updates   []V1CompressableQueryUpdate
}

// V1CompressableQueryUpdate is a sum type for query updates.
// Tag 0 = Uncompressed(QueryUpdate), Tag 1 = Brotli(Bytes), Tag 2 = Gzip(Bytes).
type V1CompressableQueryUpdate interface {
	isV1CompressableQueryUpdate()
}

// V1Uncompressed is an uncompressed query update.
type V1Uncompressed struct {
	Deletes *BsatnRowList
	Inserts *BsatnRowList
}

func (*V1Uncompressed) isV1CompressableQueryUpdate() {}

// V1CompressedBrotli is a brotli-compressed query update.
type V1CompressedBrotli struct {
	Data []byte
}

func (*V1CompressedBrotli) isV1CompressableQueryUpdate() {}

// V1CompressedGzip is a gzip-compressed query update.
type V1CompressedGzip struct {
	Data []byte
}

func (*V1CompressedGzip) isV1CompressableQueryUpdate() {}

// OneOffQueryResponse is sent in response to a OneOffQuery (tag 4 in v1).
type OneOffQueryResponse struct {
	MessageID []byte
	Error     *string // Option<String>
	Tables    []V1OneOffTable
	Duration  types.TimeDuration
}

func (*OneOffQueryResponse) serverMessageTag() uint8 { return 4 }

// V1OneOffTable is a table in a OneOffQueryResponse.
type V1OneOffTable struct {
	TableName string
	Rows      *BsatnRowList
}

// SubscribeMultiApplied is sent in response to a SubscribeMulti message (tag 8 in v1).
type SubscribeMultiApplied struct {
	RequestID uint32
	Duration  uint64
	QueryID   uint32
	Update    V1DatabaseUpdate
}

func (*SubscribeMultiApplied) serverMessageTag() uint8 { return 8 }

// UnsubscribeMultiApplied is sent in response to an UnsubscribeMulti message (tag 9 in v1).
type UnsubscribeMultiApplied struct {
	RequestID uint32
	Duration  uint64
	QueryID   uint32
	Update    V1DatabaseUpdate
}

func (*UnsubscribeMultiApplied) serverMessageTag() uint8 { return 9 }

// V1SubscriptionError notifies the client of a subscription failure (tag 7 in v1).
type V1SubscriptionError struct {
	Duration  uint64
	RequestID *uint32
	QueryID   *uint32
	TableID   *uint32
	Error     string
}

func (*V1SubscriptionError) serverMessageTag() uint8 { return 7 }

// ReadServerMessage reads a v1 ServerMessage from a BSATN reader by dispatching on the sum type tag.
func ReadServerMessage(r bsatn.Reader) (ServerMessage, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}

	switch tag {
	case 0:
		return readInitialSubscription(r)
	case 1:
		return readV1TransactionUpdate(r)
	case 2:
		return readV1TransactionUpdateLight(r)
	case 3:
		return readIdentityToken(r)
	case 4:
		return readOneOffQueryResponse(r)
	case 5:
		return readV1SubscribeApplied(r)
	case 6:
		return readV1UnsubscribeApplied(r)
	case 7:
		return readV1SubscriptionError(r)
	case 8:
		return readSubscribeMultiApplied(r)
	case 9:
		return readUnsubscribeMultiApplied(r)
	case 10:
		return readV1ProcedureResult(r)
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "ServerMessage"}
	}
}
