package client

import (
	"fmt"
	"strings"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

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

// reducerErrMessage decodes the payload a failed reducer returns. The module
// writes its error text through the host as a BSATN string, so that is tried
// first; a payload that is not a BSATN string is taken as raw UTF-8, which keeps
// hosts that write the message plainly working. A reducer that failed without
// saying why yields a fixed sentence, so the caller always has something to show.
func reducerErrMessage(value []byte) string {
	if decoded, err := bsatn.DecodeString(value); err == nil {
		// A successful decode is authoritative: falling back to the raw bytes
		// here would hand the caller the length prefix as if it were a message.
		return reducerErrOrFallback(decoded)
	}
	return reducerErrOrFallback(string(value))
}

// reducerErrFallback is shown when the server gave no reason at all.
const reducerErrFallback = "reducer returned an error"

func reducerErrOrFallback(msg string) string {
	if trimmed := strings.TrimSpace(msg); trimmed != "" {
		return trimmed
	}
	return reducerErrFallback
}
