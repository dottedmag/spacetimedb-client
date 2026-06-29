package client

import "fmt"

// ConnectionError represents a WebSocket connection or transport failure.
// It wraps an underlying error accessible via [errors.Unwrap].
// Returned by [DbConnectionBuilder.Build] and [DbConnection.Run] when the
// connection cannot be established or is unexpectedly lost.
type ConnectionError struct {
	Message string
	Err     error
}

func (e *ConnectionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("connection error: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("connection error: %s", e.Message)
}

func (e *ConnectionError) Unwrap() error { return e.Err }

// ReducerError represents a server-side reducer execution failure.
// It is produced when the server reports a reducer call with status "failed".
type ReducerError struct {
	ReducerName string
	Message     string
}

func (e *ReducerError) Error() string {
	return fmt.Sprintf("reducer %s error: %s", e.ReducerName, e.Message)
}

// ProcedureError represents a server-side procedure execution failure.
// It is produced when the server reports a procedure call with a non-success
// status: an internal/host error (e.g. a type error or unknown procedure name)
// or an out-of-energy interruption.
type ProcedureError struct {
	ProcedureName string
	Message       string
}

func (e *ProcedureError) Error() string {
	if e.ProcedureName != "" {
		return fmt.Sprintf("procedure %s error: %s", e.ProcedureName, e.Message)
	}
	return fmt.Sprintf("procedure error: %s", e.Message)
}

// SubscriptionError represents a server-side subscription failure.
// It is produced when the server sends a V1SubscriptionError message
// in response to a Subscribe or SubscribeMulti request.
type SubscriptionError struct {
	QuerySetID uint32
	Message    string
}

func (e *SubscriptionError) Error() string {
	return fmt.Sprintf("subscription %d error: %s", e.QuerySetID, e.Message)
}

// ProtocolError represents a BSATN protocol encoding or decoding failure.
// It may wrap an underlying error accessible via [errors.Unwrap].
// Produced when a server message cannot be decompressed, decoded, or
// when a one-off query response contains a server-side error string.
type ProtocolError struct {
	Message string
	Err     error
}

func (e *ProtocolError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("protocol error: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("protocol error: %s", e.Message)
}

func (e *ProtocolError) Unwrap() error { return e.Err }
