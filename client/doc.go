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
// # Cache update boundaries
//
// [DbConnectionBuilder.OnTxStart] and [DbConnectionBuilder.OnTxFinish] bracket
// each complete cache update batch, including its row and subscription callbacks.
// A batch contains all query sets changed by a committed transaction, or an
// initial/replayed subscription snapshot, unsubscribe, or subscription cleanup.
// A batched subscription response has one boundary around all its results.
// These are local application boundaries; the remote transaction already committed.
// Independent connections do not share a database transaction offset.
//
// Row callbacks may observe a partly applied batch. A successful finish callback
// sees the complete result before Run applies another batch. Finish is also
// called on row decoding failure with a nonnil error: the cache may then be
// partially updated and must not be consumed; Run terminates that session.
//
// To make reads from other goroutines coherent, acquire a shared mutex in start
// and release it in finish. Readers hold the same mutex across related cache
// reads. Callbacks inside the batch already hold it and must not acquire it again.
// Do not wait for SDK responses in these callbacks or while holding that mutex.
// Transport, lifecycle callbacks, and procedure/query response delivery are outside
// the cache update boundaries. Reconnect snapshots reflect current state rather
// than replaying every transaction missed while disconnected.
//
// # Subscriptions
//
// For a long-lived subscription, [DbConnectionBuilder.RunSubscription] owns
// initial connection retries, event-loop startup, subscription application,
// automatic transport reconnects, and fresh sessions after terminal errors.
// Initial connection and application have ten-second deadlines. Reconnect dials
// and their identity-message waits inherit the 24-hour session deadline; after
// identity confirmation, replay application has a ten-second deadline. Cancel
// the runner's context to stop and join the session.
// A successful Observe callback also finishes the runner. Mutations are never
// retried by the runner: their ordering and unknown outcomes remain the caller's
// responsibility.
//
// [ManagedSubscriptionOptions.Initialize] registers table definitions before
// the receive loop starts. The optional Observe callback starts after the first
// complete application and runs separately from the receive loop, so it may
// wait for procedure responses. It must return when its context is cancelled.
//
// [ManagedSubscription.Ready] becomes true only after a successful application
// batch. It becomes false on disconnect, decode failure, subscription failure,
// and retirement. Managed sessions use the configured OnTxStart/OnTxFinish hooks
// for readiness invalidation too, including when no cache rows change. Observe
// Ready and related cache rows under the same mutex used by those hooks. The
// coalesced Changes channel is notified after OnTxFinish releases that mutex.
// These signals do not preserve every intermediate transaction across reconnects.
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
