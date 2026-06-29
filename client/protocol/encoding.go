package protocol

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// This file implements decoding for the v2 ServerMessage sum type and its
// nested row/update types. The dispatch entry point is ReadServerMessage in
// server_message.go.

// readInitialConnection reads an InitialConnection (tag 0).
// Fields: identity(32B), connection_id(16B), token(String).
func readInitialConnection(r bsatn.Reader) (ServerMessage, error) {
	identity, err := types.ReadIdentity(r)
	if err != nil {
		return nil, err
	}
	connID, err := types.ReadConnectionId(r)
	if err != nil {
		return nil, err
	}
	token, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &InitialConnection{Identity: identity, ConnectionID: connID, Token: token}, nil
}

// readSubscribeApplied reads a SubscribeApplied (tag 1).
// Fields: request_id(u32), query_set_id(u32), rows(QueryRows).
func readSubscribeApplied(r bsatn.Reader) (ServerMessage, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	querySetID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	rows, err := readQueryRows(r)
	if err != nil {
		return nil, err
	}
	return &SubscribeApplied{RequestID: requestID, QuerySetID: querySetID, Rows: rows}, nil
}

// readUnsubscribeApplied reads an UnsubscribeApplied (tag 2).
// Fields: request_id(u32), query_set_id(u32), rows(Option<QueryRows>).
func readUnsubscribeApplied(r bsatn.Reader) (ServerMessage, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	querySetID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	rows, err := bsatn.ReadOption(r, readQueryRows)
	if err != nil {
		return nil, err
	}
	return &UnsubscribeApplied{RequestID: requestID, QuerySetID: querySetID, Rows: rows}, nil
}

// readSubscriptionError reads a SubscriptionError (tag 3).
// Fields: request_id(Option<u32>), query_set_id(u32), error(String).
func readSubscriptionError(r bsatn.Reader) (ServerMessage, error) {
	requestID, err := bsatn.ReadOption(r, func(r bsatn.Reader) (uint32, error) { return r.GetU32() })
	if err != nil {
		return nil, err
	}
	querySetID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	errMsg, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &SubscriptionError{RequestID: requestID, QuerySetID: querySetID, Error: errMsg}, nil
}

// readTransactionUpdate reads a TransactionUpdate (tag 4) as a ServerMessage.
func readTransactionUpdate(r bsatn.Reader) (ServerMessage, error) {
	tu, err := readTransactionUpdateValue(r)
	if err != nil {
		return nil, err
	}
	return &tu, nil
}

// readTransactionUpdateValue reads a TransactionUpdate value. It is used both as
// a top-level server message and nested inside a ReducerOk.
// Fields: query_sets(Vec<QuerySetUpdate>).
func readTransactionUpdateValue(r bsatn.Reader) (TransactionUpdate, error) {
	n, err := r.GetArrayLen()
	if err != nil {
		return TransactionUpdate{}, err
	}
	querySets := make([]QuerySetUpdate, n)
	for i := uint32(0); i < n; i++ {
		qs, err := readQuerySetUpdate(r)
		if err != nil {
			return TransactionUpdate{}, err
		}
		querySets[i] = qs
	}
	return TransactionUpdate{QuerySets: querySets}, nil
}

// readOneOffQueryResult reads a OneOffQueryResult (tag 5).
// Fields: request_id(u32), result(Result<QueryRows, String>).
func readOneOffQueryResult(r bsatn.Reader) (ServerMessage, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // Ok(QueryRows)
		rows, err := readQueryRows(r)
		if err != nil {
			return nil, err
		}
		return &OneOffQueryResult{RequestID: requestID, Rows: &rows}, nil
	case 1: // Err(String)
		msg, err := r.GetString()
		if err != nil {
			return nil, err
		}
		return &OneOffQueryResult{RequestID: requestID, Error: &msg}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "OneOffQueryResult.result"}
	}
}

// readReducerResult reads a ReducerResult (tag 6).
// Fields: request_id(u32), timestamp(Timestamp), result(ReducerOutcome).
func readReducerResult(r bsatn.Reader) (ServerMessage, error) {
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	timestamp, err := types.ReadTimestamp(r)
	if err != nil {
		return nil, err
	}
	outcome, err := readReducerOutcome(r)
	if err != nil {
		return nil, err
	}
	return &ReducerResult{RequestID: requestID, Timestamp: timestamp, Outcome: outcome}, nil
}

// readReducerOutcome reads a ReducerOutcome sum type.
// Tag 0 = Ok(ReducerOk), Tag 1 = OkEmpty, Tag 2 = Err(Bytes), Tag 3 = InternalError(String).
func readReducerOutcome(r bsatn.Reader) (ReducerOutcome, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // Ok(ReducerOk { ret_value: Bytes, transaction_update: TransactionUpdate })
		retValue, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		tu, err := readTransactionUpdateValue(r)
		if err != nil {
			return nil, err
		}
		return &ReducerOk{RetValue: retValue, TransactionUpdate: tu}, nil
	case 1: // OkEmpty
		return &ReducerOkEmpty{}, nil
	case 2: // Err(Bytes)
		v, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		return &ReducerErr{Value: v}, nil
	case 3: // InternalError(String)
		msg, err := r.GetString()
		if err != nil {
			return nil, err
		}
		return &ReducerInternalError{Message: msg}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "ReducerOutcome"}
	}
}

// readProcedureResult reads a ProcedureResult (tag 7).
// Fields: status(ProcedureStatus), timestamp(Timestamp), total_host_execution_duration(TimeDuration), request_id(u32).
func readProcedureResult(r bsatn.Reader) (ServerMessage, error) {
	status, err := readProcedureStatus(r)
	if err != nil {
		return nil, err
	}
	timestamp, err := types.ReadTimestamp(r)
	if err != nil {
		return nil, err
	}
	duration, err := types.ReadTimeDuration(r)
	if err != nil {
		return nil, err
	}
	requestID, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	return &ProcedureResult{
		Status:                     status,
		Timestamp:                  timestamp,
		TotalHostExecutionDuration: duration,
		RequestID:                  requestID,
	}, nil
}

// readProcedureStatus reads a ProcedureStatus sum type.
// Tag 0 = Returned(Bytes), Tag 1 = InternalError(String).
func readProcedureStatus(r bsatn.Reader) (ProcedureStatus, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // Returned(Bytes)
		v, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		return &ProcedureReturned{Value: v}, nil
	case 1: // InternalError(String)
		msg, err := r.GetString()
		if err != nil {
			return nil, err
		}
		return &ProcedureInternalError{Message: msg}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "ProcedureStatus"}
	}
}

// --- nested row/update readers ---

// readQueryRows reads a QueryRows value: tables(Vec<SingleTableRows>).
func readQueryRows(r bsatn.Reader) (QueryRows, error) {
	n, err := r.GetArrayLen()
	if err != nil {
		return QueryRows{}, err
	}
	tables := make([]SingleTableRows, n)
	for i := uint32(0); i < n; i++ {
		st, err := readSingleTableRows(r)
		if err != nil {
			return QueryRows{}, err
		}
		tables[i] = st
	}
	return QueryRows{Tables: tables}, nil
}

// readSingleTableRows reads a SingleTableRows value: table(String), rows(BsatnRowList).
func readSingleTableRows(r bsatn.Reader) (SingleTableRows, error) {
	name, err := r.GetString()
	if err != nil {
		return SingleTableRows{}, err
	}
	rows, err := ReadBsatnRowList(r)
	if err != nil {
		return SingleTableRows{}, err
	}
	return SingleTableRows{TableName: name, Rows: rows}, nil
}

// readQuerySetUpdate reads a QuerySetUpdate value: query_set_id(u32), tables(Vec<TableUpdate>).
func readQuerySetUpdate(r bsatn.Reader) (QuerySetUpdate, error) {
	querySetID, err := r.GetU32()
	if err != nil {
		return QuerySetUpdate{}, err
	}
	n, err := r.GetArrayLen()
	if err != nil {
		return QuerySetUpdate{}, err
	}
	tables := make([]TableUpdate, n)
	for i := uint32(0); i < n; i++ {
		tu, err := readTableUpdate(r)
		if err != nil {
			return QuerySetUpdate{}, err
		}
		tables[i] = tu
	}
	return QuerySetUpdate{QuerySetID: querySetID, Tables: tables}, nil
}

// readTableUpdate reads a TableUpdate value: table_name(String), rows(Vec<TableUpdateRows>).
func readTableUpdate(r bsatn.Reader) (TableUpdate, error) {
	name, err := r.GetString()
	if err != nil {
		return TableUpdate{}, err
	}
	n, err := r.GetArrayLen()
	if err != nil {
		return TableUpdate{}, err
	}
	rows := make([]TableUpdateRows, n)
	for i := uint32(0); i < n; i++ {
		tur, err := readTableUpdateRows(r)
		if err != nil {
			return TableUpdate{}, err
		}
		rows[i] = tur
	}
	return TableUpdate{TableName: name, Rows: rows}, nil
}

// readTableUpdateRows reads a TableUpdateRows sum type.
// Tag 0 = PersistentTable(inserts, deletes), Tag 1 = EventTable(events).
func readTableUpdateRows(r bsatn.Reader) (TableUpdateRows, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // PersistentTable { inserts: BsatnRowList, deletes: BsatnRowList }
		inserts, err := ReadBsatnRowList(r)
		if err != nil {
			return nil, err
		}
		deletes, err := ReadBsatnRowList(r)
		if err != nil {
			return nil, err
		}
		return &PersistentTableRows{Inserts: inserts, Deletes: deletes}, nil
	case 1: // EventTable { events: BsatnRowList }
		events, err := ReadBsatnRowList(r)
		if err != nil {
			return nil, err
		}
		return &EventTableRows{Events: events}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "TableUpdateRows"}
	}
}
