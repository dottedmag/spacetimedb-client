// Package bsatn implements BSATN (Binary SpacetimeDB Algebraic Type Notation),
// a deterministic, little-endian binary serialization format used by SpacetimeDB
// for all wire and storage encoding.
//
// BSATN encodes values from the SpacetimeDB algebraic type system: primitives,
// products (structs), sum types (tagged unions), arrays, maps, and options.
// The format is self-describing only when paired with a schema; the raw bytes
// contain no type tags or field names.
//
// # Encoding Rules
//
// Primitives are encoded in little-endian byte order at their natural width:
//
//   - bool: 1 byte (0x00 = false, 0x01 = true)
//   - u8 / i8: 1 byte
//   - u16 / i16: 2 bytes LE
//   - u32 / i32: 4 bytes LE
//   - u64 / i64: 8 bytes LE
//   - f32: 4 bytes LE (IEEE 754)
//   - f64: 8 bytes LE (IEEE 754)
//
// Strings are encoded as a u32 LE byte-length prefix followed by raw UTF-8 bytes.
//
// Arrays are encoded as a u32 LE element count followed by each element encoded
// sequentially. Byte arrays follow the same layout (count + raw bytes).
//
// Maps are encoded as a u32 LE entry count followed by key-value pairs encoded
// sequentially.
//
// Products (structs) are encoded as their fields written sequentially with no
// length prefix or separator. Field order must match the schema exactly.
//
// Sum types (tagged unions) are encoded as a u8 tag byte followed by the
// variant payload. Unit variants have no payload.
//
// Options are a special sum type: tag 0 = Some(value), tag 1 = None (no payload).
// Go represents Option<T> as *T, where nil means None.
//
// # Core Interfaces
//
// [Writer] is a streaming encoder that accumulates BSATN bytes into a buffer.
// Create one with [NewWriter]:
//
//	w := bsatn.NewWriter(64)
//	w.PutU32(42)
//	w.PutString("hello")
//	data := w.Bytes()
//
// [Reader] is a streaming decoder that reads BSATN bytes from a buffer.
// Create one with [NewReader] or [NewZeroCopyReader]:
//
//	r := bsatn.NewReader(data)
//	id, err := r.GetU32()
//	name, err := r.GetString()
//
// [NewZeroCopyReader] returns a [Reader] whose GetString method returns strings
// that share the underlying buffer memory, avoiding a copy. The data buffer
// must outlive all decoded strings.
//
// [Serializable] is implemented by types that can encode themselves as BSATN.
// It requires a single method:
//
//	WriteBsatn(w Writer)
//
// # Implementing Serializable for Custom Structs
//
// Product types (structs) implement [Serializable] by writing each field in
// schema order. Provide a corresponding read function for decoding:
//
//	type player struct {
//		id   uint64
//		name string
//		hp   uint32
//	}
//
//	func (p *player) WriteBsatn(w bsatn.Writer) {
//		w.PutU64(p.id)
//		w.PutString(p.name)
//		w.PutU32(p.hp)
//	}
//
//	func readPlayer(r bsatn.Reader) (player, error) {
//		id, err := r.GetU64()
//		if err != nil {
//			return player{}, err
//		}
//		name, err := r.GetString()
//		if err != nil {
//			return player{}, err
//		}
//		hp, err := r.GetU32()
//		if err != nil {
//			return player{}, err
//		}
//		return player{id: id, name: name, hp: hp}, nil
//	}
//
// Sum types implement [Serializable] using [WriteSum] or [WriteSumUnit]:
//
//	func (m *chatMessage) WriteBsatn(w bsatn.Writer) {
//		switch v := m.variant.(type) {
//		case *textMessage:
//			bsatn.WriteSum(w, 0, v)
//		case *systemMessage:
//			bsatn.WriteSum(w, 1, v)
//		}
//	}
//
// # Generic Helpers
//
// The package provides generic functions for encoding and decoding composite
// types without hand-writing loops:
//
//   - [Encode] serializes any [Serializable] value to a byte slice.
//   - [Decode] deserializes a byte slice using a provided read function.
//   - [WriteArray] / [ReadArray] handle slices of [Serializable] elements.
//   - [WriteByteArray] / [ReadByteArray] handle []byte efficiently.
//   - [WriteOption] / [ReadOption] handle *T as Option<T>.
//   - [WriteMap] / [ReadMap] handle maps with typed key-value pairs.
//   - [WriteSum] / [WriteSumUnit] handle sum type variant encoding.
//
// Example using Encode and Decode:
//
//	data := bsatn.Encode(&myPlayer)
//	decoded, err := bsatn.Decode(data, readPlayer)
//
// There are also typed primitive helpers for one-shot encode/decode of
// individual values: [EncodeBool], [DecodeBool], [EncodeU32], [DecodeU32],
// [EncodeString], [DecodeString], and so on for each primitive type.
//
// # Go-to-BSATN Type Mapping
//
//	Go type          BSATN type    Wire size
//	bool             bool          1 byte
//	uint8            u8            1 byte
//	uint16           u16           2 bytes LE
//	uint32           u32           4 bytes LE
//	uint64           u64           8 bytes LE
//	int8             i8            1 byte
//	int16            i16           2 bytes LE
//	int32            i32           4 bytes LE
//	int64            i64           8 bytes LE
//	float32          f32           4 bytes LE
//	float64          f64           8 bytes LE
//	string           String        4-byte len + UTF-8 bytes
//	[]byte           Array<u8>     4-byte len + raw bytes
//	[]T              Array<T>      4-byte count + elements
//	*T               Option<T>     1-byte tag [+ value]
//	map[K]V          Map<K,V>      4-byte count + key-value pairs
//	struct           Product       sequential fields
//	tagged union     Sum           1-byte tag + payload
//
// # Errors
//
// Reader methods return typed errors when decoding fails:
//
//   - [ErrBufferTooShort]: not enough bytes remain for the requested type.
//   - [ErrInvalidBool]: a bool byte is not 0x00 or 0x01.
//   - [ErrInvalidTag]: a sum type tag does not match any known variant.
package bsatn
