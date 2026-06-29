// Package ws provides low-level WebSocket connection management for
// communicating with a SpacetimeDB server.
//
// # Interfaces
//
// [Connection] represents an open WebSocket link and exposes methods for
// sending and receiving binary messages:
//
//   - Send transmits a BSATN-encoded client message.
//   - Recv blocks until the next server message arrives.
//   - Close performs a graceful WebSocket shutdown.
//
// [ConnectionBuilder] configures and establishes a [Connection] using the
// builder pattern.
//
// # Protocol Negotiation
//
// Connections negotiate the v2.bsatn.spacetimedb WebSocket subprotocol by
// default. The subprotocol can be overridden with
// [ConnectionBuilder.WithProtocol] if needed.
//
// # Builder Pattern
//
// Create a connection by chaining builder methods:
//
//	conn, err := ws.NewConnection().
//	    WithUri("ws://localhost:3000").
//	    WithDatabaseName("my_database").
//	    WithToken(authToken).
//	    Build(ctx)
//
// # Usage
//
// Most users do not need to use this package directly. The higher-level
// [go.digitalxero.dev/spacetimedb-client/client.DbConnection] manages
// WebSocket connections internally, handling reconnection, message framing,
// and subscription lifecycle.
package ws
