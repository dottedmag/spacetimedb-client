// Package types provides domain types that map to SpacetimeDB's algebraic type
// system (SATS). Every type in this package corresponds to a first-class concept
// in the SpacetimeDB runtime and is serializable over the BSATN binary wire
// format via the [go.digitalxero.dev/spacetimedb-client/bsatn.Serializable]
// interface.
//
// # Identifiers
//
// Three fixed-size byte types identify entities in SpacetimeDB:
//
//   - [Identity] (32 bytes) identifies a user. It is a product-wrapped u256.
//   - [ConnectionId] (16 bytes) identifies a client connection. It is a
//     product-wrapped u128.
//   - [Uuid] (16 bytes) is a standard UUID stored as a product-wrapped u128.
//
// All three store their bytes in little-endian order internally but render as
// big-endian hexadecimal in their String methods to match the canonical display
// format used by the SpacetimeDB CLI and dashboard.
//
// # Extended Integers
//
// SpacetimeDB supports integer widths beyond what Go provides natively:
//
//   - [Uint128] and [Int128] are 128-bit integers (16 bytes, little-endian).
//   - [Uint256] and [Int256] are 256-bit integers (32 bytes, little-endian).
//
// Signed variants use two's complement representation. Each exposes Lo/Hi
// accessors (128-bit) or a Bytes accessor (256-bit) and converts to
// [math/big.Int] via its String method.
//
// # Time
//
//   - [Timestamp] wraps an i64 representing microseconds since the Unix epoch.
//     Use its Time method to convert to a standard [time.Time].
//   - [TimeDuration] wraps an i64 representing a duration in microseconds.
//     Use its Duration method to convert to a standard [time.Duration].
//
// # Energy
//
// [EnergyQuanta] wraps a [Uint128] representing the u128 energy budget
// available for a reducer call. Energy is the unit SpacetimeDB uses to meter
// computation.
//
// # Scheduling
//
// [ScheduleAt] is a sum type (tagged union) with two variants:
//
//   - [ScheduleAtInterval] (tag 0) holds a [TimeDuration] for recurring
//     execution.
//   - [ScheduleAtTime] (tag 1) holds a [Timestamp] for one-shot execution at a
//     specific point in time.
//
// # Algebraic Type System
//
// The remaining types model SpacetimeDB's algebraic type system (SATS):
//
//   - [AlgebraicType] is the top-level type descriptor. Primitive constructors
//     such as [AlgTypeBool], [AlgTypeU8], [AlgTypeString], etc. return leaf
//     types. Compound types are built with [AlgTypeProduct], [AlgTypeSum], and
//     [AlgTypeRef].
//   - [ProductType] describes a struct-like type with ordered, named fields
//     represented by [ProductTypeElement] values.
//   - [SumType] describes an enum/union type with named variants represented
//     by [SumTypeVariant] values.
//   - [TypeRef] is an index into a [Typespace], enabling recursive and
//     mutually-referential type definitions.
//   - [Typespace] is a registry that maps [TypeRef] indices to
//     [AlgebraicType] definitions. Use [Typespace.Reserve] and [Typespace.Set]
//     for forward references needed by recursive types.
//
// # Constructor Conventions
//
// Every type follows a consistent pair of constructor functions:
//
//   - NewXxx creates a value from Go-native arguments (e.g., [NewIdentity],
//     [NewTimestamp], [NewUint128]).
//   - ReadXxx deserializes a value from a [go.digitalxero.dev/spacetimedb-client/bsatn.Reader]
//     (e.g., [ReadIdentity], [ReadTimestamp], [ReadUint128]).
//
// All types implement [go.digitalxero.dev/spacetimedb-client/bsatn.Serializable]
// (the WriteBsatn method) and most provide a String method for human-readable
// output.
package types
