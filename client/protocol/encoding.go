package protocol

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/andybalholm/brotli"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// readIdentityToken reads an IdentityToken (tag 3) from a BSATN reader.
// Fields: identity(32B), token(string), connection_id(16B).
func readIdentityToken(r bsatn.Reader) (*IdentityToken, error) {
	identity, err := types.ReadIdentity(r)
	if err != nil {
		return nil, err
	}

	token, err := r.GetString()
	if err != nil {
		return nil, err
	}

	connID, err := types.ReadConnectionId(r)
	if err != nil {
		return nil, err
	}

	return &IdentityToken{
		Identity:     identity,
		Token:        token,
		ConnectionID: connID,
	}, nil
}

// readInitialSubscription reads an InitialSubscription (tag 0) from a BSATN reader.
// Fields: database_update(DatabaseUpdate), request_id(u32), total_host_execution_duration(TimeDuration).
func readInitialSubscription(r bsatn.Reader) (*InitialSubscription, error) {
	update, err := readV1DatabaseUpdate(r)
	if err != nil {
		return nil, err
	}

	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	duration, err := types.ReadTimeDuration(r)
	if err != nil {
		return nil, err
	}

	return &InitialSubscription{
		DatabaseUpdate: *update,
		RequestID:      requestID,
		Duration:       duration,
	}, nil
}

// readV1TransactionUpdate reads a TransactionUpdate (tag 1) from a BSATN reader.
// Fields: status(UpdateStatus), timestamp(i64), caller_identity(32B), caller_connection_id(16B),
//
//	reducer_call(ReducerCallInfo), energy_quanta_used(EnergyQuanta), total_host_execution_duration(TimeDuration).
func readV1TransactionUpdate(r bsatn.Reader) (*V1TransactionUpdate, error) {
	status, err := readV1UpdateStatus(r)
	if err != nil {
		return nil, err
	}

	timestamp, err := types.ReadTimestamp(r)
	if err != nil {
		return nil, err
	}

	callerIdentity, err := types.ReadIdentity(r)
	if err != nil {
		return nil, err
	}

	callerConnID, err := types.ReadConnectionId(r)
	if err != nil {
		return nil, err
	}

	reducerCall, err := readV1ReducerCallInfo(r)
	if err != nil {
		return nil, err
	}

	energyQuanta, err := types.ReadEnergyQuanta(r)
	if err != nil {
		return nil, err
	}

	duration, err := types.ReadTimeDuration(r)
	if err != nil {
		return nil, err
	}

	return &V1TransactionUpdate{
		Status:                     status,
		Timestamp:                  timestamp,
		CallerIdentity:             callerIdentity,
		CallerConnectionID:         callerConnID,
		ReducerCall:                *reducerCall,
		EnergyQuantaUsed:           energyQuanta,
		TotalHostExecutionDuration: duration,
	}, nil
}

// readV1TransactionUpdateLight reads a TransactionUpdateLight (tag 2) from a BSATN reader.
// Fields: request_id(u32), update(DatabaseUpdate).
func readV1TransactionUpdateLight(r bsatn.Reader) (*V1TransactionUpdateLight, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	update, err := readV1DatabaseUpdate(r)
	if err != nil {
		return nil, err
	}

	return &V1TransactionUpdateLight{
		RequestID:      requestID,
		DatabaseUpdate: *update,
	}, nil
}

// readV1UpdateStatus reads an UpdateStatus sum type.
// Tag 0 = Committed(DatabaseUpdate), Tag 1 = Failed(String), Tag 2 = OutOfEnergy.
func readV1UpdateStatus(r bsatn.Reader) (V1UpdateStatus, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}

	switch tag {
	case 0: // Committed
		update, err := readV1DatabaseUpdate(r)
		if err != nil {
			return nil, err
		}
		return &V1Committed{DatabaseUpdate: *update}, nil
	case 1: // Failed
		msg, err := r.GetString()
		if err != nil {
			return nil, err
		}
		return &V1Failed{Message: msg}, nil
	case 2: // OutOfEnergy
		return &V1OutOfEnergy{}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "UpdateStatus"}
	}
}

// readV1ReducerCallInfo reads a ReducerCallInfo from a BSATN reader.
// Fields: reducer_name(String), reducer_id(u32), args(Bytes), request_id(u32).
func readV1ReducerCallInfo(r bsatn.Reader) (*V1ReducerCallInfo, error) {
	name, err := r.GetString()
	if err != nil {
		return nil, err
	}

	reducerID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	// args in BSATN format is Box<[u8]> which serializes as byte array
	args, err := bsatn.ReadByteArray(r)
	if err != nil {
		return nil, err
	}

	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	return &V1ReducerCallInfo{
		ReducerName: name,
		ReducerID:   reducerID,
		Args:        args,
		RequestID:   requestID,
	}, nil
}

// readV1DatabaseUpdate reads a DatabaseUpdate from a BSATN reader.
// Fields: tables(Vec<TableUpdate>).
func readV1DatabaseUpdate(r bsatn.Reader) (*V1DatabaseUpdate, error) {
	tables, err := bsatn.ReadArray(r, readV1TableUpdate)
	if err != nil {
		return nil, err
	}
	return &V1DatabaseUpdate{Tables: tables}, nil
}

// readV1TableUpdate reads a v1 TableUpdate from a BSATN reader.
// Fields: table_id(u32), table_name(String), num_rows(u64), updates(Vec<CompressableQueryUpdate>).
func readV1TableUpdate(r bsatn.Reader) (V1TableUpdate, error) {
	tableID, err := r.GetU32()
	if err != nil {
		return V1TableUpdate{}, err
	}

	tableName, err := r.GetString()
	if err != nil {
		return V1TableUpdate{}, err
	}

	numRows, err := r.GetU64()
	if err != nil {
		return V1TableUpdate{}, err
	}

	// SmallVec<[CompressableQueryUpdate; 1]> serializes as a regular Vec
	updates, err := bsatn.ReadArray(r, readV1CompressableQueryUpdate)
	if err != nil {
		return V1TableUpdate{}, err
	}

	return V1TableUpdate{
		TableID:   tableID,
		TableName: tableName,
		NumRows:   numRows,
		Updates:   updates,
	}, nil
}

// readV1CompressableQueryUpdate reads a CompressableQueryUpdate sum type.
// Tag 0 = Uncompressed(QueryUpdate), Tag 1 = Brotli(Bytes), Tag 2 = Gzip(Bytes).
func readV1CompressableQueryUpdate(r bsatn.Reader) (V1CompressableQueryUpdate, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}

	switch tag {
	case 0: // Uncompressed(QueryUpdate { deletes, inserts })
		deletes, err := ReadBsatnRowList(r)
		if err != nil {
			return nil, err
		}
		inserts, err := ReadBsatnRowList(r)
		if err != nil {
			return nil, err
		}
		return &V1Uncompressed{Deletes: deletes, Inserts: inserts}, nil
	case 1: // Brotli(Bytes)
		data, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		return &V1CompressedBrotli{Data: data}, nil
	case 2: // Gzip(Bytes)
		data, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		return &V1CompressedGzip{Data: data}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "CompressableQueryUpdate"}
	}
}

// DecompressV1QueryUpdate decompresses a V1CompressableQueryUpdate into an Uncompressed update.
func DecompressV1QueryUpdate(cqu V1CompressableQueryUpdate) (*V1Uncompressed, error) {
	switch u := cqu.(type) {
	case *V1Uncompressed:
		return u, nil
	case *V1CompressedBrotli:
		decoded, err := io.ReadAll(brotli.NewReader(bytes.NewReader(u.Data)))
		if err != nil {
			return nil, err
		}
		return parseQueryUpdate(decoded)
	case *V1CompressedGzip:
		gr, err := gzip.NewReader(bytes.NewReader(u.Data))
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		decoded, err := io.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		return parseQueryUpdate(decoded)
	}
	return nil, nil
}

// parseQueryUpdate parses a BSATN-encoded QueryUpdate { deletes, inserts }.
func parseQueryUpdate(data []byte) (*V1Uncompressed, error) {
	r := bsatn.NewReader(data)
	deletes, err := ReadBsatnRowList(r)
	if err != nil {
		return nil, err
	}
	inserts, err := ReadBsatnRowList(r)
	if err != nil {
		return nil, err
	}
	return &V1Uncompressed{Deletes: deletes, Inserts: inserts}, nil
}

// readOneOffQueryResponse reads a OneOffQueryResponse (tag 4) from a BSATN reader.
// Fields: message_id(Bytes), error(Option<String>), tables(Vec<OneOffTable>), total_host_execution_duration(TimeDuration).
func readOneOffQueryResponse(r bsatn.Reader) (*OneOffQueryResponse, error) {
	messageID, err := bsatn.ReadByteArray(r)
	if err != nil {
		return nil, err
	}

	errorMsg, err := bsatn.ReadOption(r, func(r bsatn.Reader) (string, error) {
		return r.GetString()
	})
	if err != nil {
		return nil, err
	}

	tables, err := bsatn.ReadArray(r, readV1OneOffTable)
	if err != nil {
		return nil, err
	}

	duration, err := types.ReadTimeDuration(r)
	if err != nil {
		return nil, err
	}

	return &OneOffQueryResponse{
		MessageID: messageID,
		Error:     errorMsg,
		Tables:    tables,
		Duration:  duration,
	}, nil
}

// readV1OneOffTable reads a OneOffTable from a BSATN reader.
// Fields: table_name(String), rows(BsatnRowList).
func readV1OneOffTable(r bsatn.Reader) (V1OneOffTable, error) {
	tableName, err := r.GetString()
	if err != nil {
		return V1OneOffTable{}, err
	}

	rows, err := ReadBsatnRowList(r)
	if err != nil {
		return V1OneOffTable{}, err
	}

	return V1OneOffTable{
		TableName: tableName,
		Rows:      rows,
	}, nil
}

// readV1SubscribeApplied reads a SubscribeApplied (tag 5) from a BSATN reader.
// Fields: request_id(u32), total_host_execution_duration_micros(u64), query_id(u32), rows(SubscribeRows).
// We skip most fields since we don't use them in the Go client currently.
func readV1SubscribeApplied(r bsatn.Reader) (*SubscribeMultiApplied, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	duration, err := r.GetU64()
	if err != nil {
		return nil, err
	}

	queryID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	// SubscribeRows: table_id(u32), table_name(String), table_rows(TableUpdate)
	// We need to read the SubscribeRows struct fields
	_, err = r.GetU32() // table_id
	if err != nil {
		return nil, err
	}
	tableName, err := r.GetString()
	if err != nil {
		return nil, err
	}
	tableUpdate, err := readV1TableUpdateInline(r, tableName)
	if err != nil {
		return nil, err
	}

	return &SubscribeMultiApplied{
		RequestID: requestID,
		Duration:  duration,
		QueryID:   queryID,
		Update:    V1DatabaseUpdate{Tables: []V1TableUpdate{*tableUpdate}},
	}, nil
}

// readV1TableUpdateInline reads the inline TableUpdate fields from SubscribeRows.
// This is a v1 TableUpdate<BsatnFormat> which has: table_id, table_name, num_rows, updates.
// But in SubscribeRows context, table_id and table_name are already read.
func readV1TableUpdateInline(r bsatn.Reader, tableName string) (*V1TableUpdate, error) {
	// TableUpdate<BsatnFormat> in SubscribeRows is actually just TableUpdate fields:
	// table_id, table_name, num_rows, updates
	tableID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	tName, err := r.GetString()
	if err != nil {
		return nil, err
	}
	_ = tableName // use the outer name for context
	numRows, err := r.GetU64()
	if err != nil {
		return nil, err
	}
	updates, err := bsatn.ReadArray(r, readV1CompressableQueryUpdate)
	if err != nil {
		return nil, err
	}
	return &V1TableUpdate{
		TableID:   tableID,
		TableName: tName,
		NumRows:   numRows,
		Updates:   updates,
	}, nil
}

// readV1UnsubscribeApplied reads an UnsubscribeApplied (tag 6) - same shape as SubscribeApplied.
func readV1UnsubscribeApplied(r bsatn.Reader) (*UnsubscribeMultiApplied, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	duration, err := r.GetU64()
	if err != nil {
		return nil, err
	}

	queryID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	// SubscribeRows: table_id(u32), table_name(String), table_rows(TableUpdate)
	_, err = r.GetU32() // table_id
	if err != nil {
		return nil, err
	}
	tableName, err := r.GetString()
	if err != nil {
		return nil, err
	}
	tableUpdate, err := readV1TableUpdateInline(r, tableName)
	if err != nil {
		return nil, err
	}

	return &UnsubscribeMultiApplied{
		RequestID: requestID,
		Duration:  duration,
		QueryID:   queryID,
		Update:    V1DatabaseUpdate{Tables: []V1TableUpdate{*tableUpdate}},
	}, nil
}

// readV1SubscriptionError reads a SubscriptionError (tag 7) from a BSATN reader.
// Fields: total_host_execution_duration_micros(u64), request_id(Option<u32>), query_id(Option<u32>),
//
//	table_id(Option<u32>), error(String).
func readV1SubscriptionError(r bsatn.Reader) (*V1SubscriptionError, error) {
	duration, err := r.GetU64()
	if err != nil {
		return nil, err
	}

	requestID, err := bsatn.ReadOption(r, func(r bsatn.Reader) (uint32, error) {
		return r.GetU32()
	})
	if err != nil {
		return nil, err
	}

	queryID, err := bsatn.ReadOption(r, func(r bsatn.Reader) (uint32, error) {
		return r.GetU32()
	})
	if err != nil {
		return nil, err
	}

	tableID, err := bsatn.ReadOption(r, func(r bsatn.Reader) (uint32, error) {
		return r.GetU32()
	})
	if err != nil {
		return nil, err
	}

	errMsg, err := r.GetString()
	if err != nil {
		return nil, err
	}

	return &V1SubscriptionError{
		Duration:  duration,
		RequestID: requestID,
		QueryID:   queryID,
		TableID:   tableID,
		Error:     errMsg,
	}, nil
}

// readSubscribeMultiApplied reads a SubscribeMultiApplied (tag 8) from a BSATN reader.
// Fields: request_id(u32), total_host_execution_duration_micros(u64), query_id(u32), update(DatabaseUpdate).
func readSubscribeMultiApplied(r bsatn.Reader) (*SubscribeMultiApplied, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	duration, err := r.GetU64()
	if err != nil {
		return nil, err
	}

	queryID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	update, err := readV1DatabaseUpdate(r)
	if err != nil {
		return nil, err
	}

	return &SubscribeMultiApplied{
		RequestID: requestID,
		Duration:  duration,
		QueryID:   queryID,
		Update:    *update,
	}, nil
}

// readUnsubscribeMultiApplied reads an UnsubscribeMultiApplied (tag 9) from a BSATN reader.
func readUnsubscribeMultiApplied(r bsatn.Reader) (*UnsubscribeMultiApplied, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	duration, err := r.GetU64()
	if err != nil {
		return nil, err
	}

	queryID, err := r.GetU32()
	if err != nil {
		return nil, err
	}

	update, err := readV1DatabaseUpdate(r)
	if err != nil {
		return nil, err
	}

	return &UnsubscribeMultiApplied{
		RequestID: requestID,
		Duration:  duration,
		QueryID:   queryID,
		Update:    *update,
	}, nil
}

// readV1ProcedureResult is a placeholder for ProcedureResult (tag 10).
// Not fully implemented since tests don't use it.
func readV1ProcedureResult(r bsatn.Reader) (ServerMessage, error) {
	// Skip all remaining bytes - not needed for current tests
	return nil, &bsatn.ErrInvalidTag{Tag: 10, SumName: "ProcedureResult (not implemented)"}
}
