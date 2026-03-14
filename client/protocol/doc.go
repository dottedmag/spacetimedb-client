// Package protocol defines the internal wire protocol types for communicating
// with a SpacetimeDB server over the v1 BSATN binary protocol.
//
// # Client Messages
//
// Messages sent from the client to the server implement [ClientMessage]:
//
//   - [CallReducer] invokes a transactional database function (reducer).
//   - [Subscribe] registers a set of SQL queries as a subscription.
//   - [OneOffQuery] executes a single query without subscribing to updates.
//   - [SubscribeMulti] subscribes to multiple queries with an associated query ID.
//   - [Unsubscribe] removes a previously registered subscription.
//   - [UnsubscribeMulti] removes a multi-query subscription.
//   - [CallProcedure] invokes a non-transactional database function.
//
// # Server Messages
//
// Messages received from the server implement [ServerMessage]:
//
//   - [IdentityToken] confirms a successful connection with identity and token.
//   - [InitialSubscription] delivers the initial matching rows for a subscription.
//   - [V1TransactionUpdate] reports a reducer execution with full metadata.
//   - [V1TransactionUpdateLight] reports a reducer execution with minimal metadata.
//   - [OneOffQueryResponse] returns the results of a one-off query.
//   - [SubscribeMultiApplied] confirms a multi-query subscription.
//   - [UnsubscribeMultiApplied] confirms removal of a multi-query subscription.
//   - [V1SubscriptionError] reports a subscription failure.
//
// Server messages are decoded by [ReadServerMessage], which dispatches on the
// BSATN sum type tag.
//
// # Row Lists
//
// [BsatnRowList] holds packed BSATN-encoded rows together with a [RowSizeHint]
// that describes how to find row boundaries within the packed data. Two hint
// strategies are supported:
//
//   - [FixedSizeHint] indicates every row is the same fixed number of bytes.
//   - [RowOffsetsHint] provides explicit byte offsets for each row start.
//
// # Compression
//
// Server messages arrive with a leading compression tag byte.
// [DecompressMessage] reads that tag and decompresses the payload using the
// indicated algorithm. The supported [Compression] constants are
// [CompressionNone], [CompressionBrotli], and [CompressionGzip].
//
// # Usage
//
// This package is primarily for internal use by the SpacetimeDB client SDK.
// Most users only need the [Compression] constants and will interact with
// the database through [go.digitalxero.dev/spacetimedb-client/client.DbConnection]
// instead.
package protocol
