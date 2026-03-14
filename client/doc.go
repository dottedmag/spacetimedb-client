// Package client is the primary entry point for connecting to a SpacetimeDB
// database from Go.
//
// # Connection lifecycle
//
// A typical session follows these steps: build a connection, register table
// definitions, run the event loop, subscribe to queries, call reducers, and
// eventually disconnect.
//
//	conn, err := client.NewDbConnection().
//	    WithUri("ws://localhost:3000").
//	    WithDatabaseName("my_db").
//	    OnConnect(func(c client.DbConnection, identity types.Identity, token string) {
//	        fmt.Println("connected, token:", token)
//	    }).
//	    Build(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	conn.RegisterTable(playerDef{})
//
//	go func() {
//	    // Run blocks on this goroutine, processing all WebSocket messages.
//	    if err := conn.Run(ctx); err != nil {
//	        log.Println("disconnected:", err)
//	    }
//	}()
//
//	sub, _ := conn.Subscribe("SELECT * FROM Player").
//	    OnApplied(func() { fmt.Println("subscription applied") }).
//	    Build()
//
//	conn.CallReducer("create_player", &CreatePlayerArgs{Name: "Alice"})
//
//	conn.Disconnect()
//
// # Builder pattern
//
// [NewDbConnection] returns a [DbConnectionBuilder] that configures the
// connection through chained method calls:
//
//   - WithUri sets the WebSocket endpoint.
//   - WithDatabaseName sets the database name or address.
//   - WithToken provides an authentication token for reconnection.
//   - WithCompression selects the wire compression algorithm.
//   - OnConnect, OnConnectError, and OnDisconnect register lifecycle callbacks.
//   - Build dials the WebSocket and returns a [DbConnection].
//
// # Event loop model
//
// [DbConnection.Run] blocks on the calling goroutine and processes every
// incoming WebSocket message in that single goroutine. All internal state
// mutations happen inside Run, so no locks are needed. Commands such as
// [DbConnection.CallReducer] and [DbConnection.Subscribe] send requests
// through an internal channel that Run consumes.
//
// # Subscriptions
//
// [DbConnection.Subscribe] accepts one or more SQL queries and returns a
// [SubscriptionBuilder]. Call OnApplied to register a callback that fires
// once the server has applied the initial subscription rows to the local
// cache, then call Build to submit the subscription and obtain a
// [SubscriptionHandle]. The handle can later be used to [SubscriptionHandle.Unsubscribe].
//
// # Reducer calls
//
// [DbConnection.CallReducer] sends a reducer invocation to the server. The
// reducer name is a string and the arguments must implement
// [bsatn.Serializable] so they can be encoded to BSATN on the wire.
//
// # One-off queries
//
// [DbConnection.OneOffQuery] executes a single SQL query and returns the
// result rows as raw BSATN byte slices. Unlike subscriptions, one-off
// queries do not update the local cache.
//
// # Error types
//
// The package defines four error types for different failure modes:
//
//   - [ConnectionError] for WebSocket connection failures.
//   - [ReducerError] for reducer invocation failures.
//   - [SubscriptionError] for subscription failures.
//   - [ProtocolError] for BSATN encoding or decoding failures.
//
// # Event contexts
//
// Callbacks receive context values that carry metadata about the event:
//
//   - [EventContext] provides the caller's Identity, ConnectionID, Timestamp,
//     and a reference to the [DbConnection].
//   - [ReducerEventContext] extends [EventContext] with the ReducerName,
//     Status, and ErrMessage fields.
//   - [ErrorContext] wraps an error for error-specific callbacks.
package client
