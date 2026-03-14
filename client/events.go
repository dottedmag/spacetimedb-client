package client

import "go.digitalxero.dev/spacetimedb-client/types"

// EventContext provides metadata about a table mutation event.
// It is embedded in callback contexts to identify who caused the change
// and when it occurred.
type EventContext struct {
	Identity     types.Identity
	ConnectionID types.ConnectionId
	Timestamp    types.Timestamp
	Conn         DbConnection
}

// ReducerEventContext extends [EventContext] with reducer-specific metadata.
// Status is either "committed" (the reducer succeeded and changes were applied)
// or "failed" (the reducer errored; see ErrMessage for details).
type ReducerEventContext struct {
	EventContext
	ReducerName string
	Status      string
	ErrMessage  string
}

// ErrorContext wraps an error for use in error-handling callbacks such as
// [DbConnectionBuilder.OnConnectError].
type ErrorContext struct {
	Err error
}
