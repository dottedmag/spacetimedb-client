package bsatn_test

import (
	"testing"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// everyPrimitiveData is a reusable encoded payload for decode benchmarks.
var everyPrimitiveData []byte

func init() {
	w := bsatn.NewWriter(256)
	// u8..u256
	w.PutU8(42)
	w.PutU16(1000)
	w.PutU32(100000)
	w.PutU64(1000000000)
	types.NewUint128(123, 456).WriteBsatn(w)
	types.NewUint256([32]byte{1, 2, 3}).WriteBsatn(w)
	// i8..i256
	w.PutI8(-42)
	w.PutI16(-1000)
	w.PutI32(-100000)
	w.PutI64(-1000000000)
	types.NewInt128(123, 456).WriteBsatn(w)
	types.NewInt256([32]byte{1, 2, 3}).WriteBsatn(w)
	// bool, f32, f64
	w.PutBool(true)
	w.PutF32(3.14)
	w.PutF64(2.71828)
	// string
	w.PutString("hello world")
	// Identity (32 bytes), ConnectionId (16 bytes), Uuid (16 bytes), Timestamp
	types.NewIdentity([32]byte{1}).WriteBsatn(w)
	types.NewConnectionId([16]byte{2}).WriteBsatn(w)
	types.NewUuid([16]byte{3}).WriteBsatn(w)
	types.NewTimestamp(1234567890).WriteBsatn(w)
	everyPrimitiveData = w.Bytes()
}

func BenchmarkEncode_U32(b *testing.B) {
	w := bsatn.NewWriter(4)
	for b.Loop() {
		w.Reset()
		w.PutU32(42)
	}
}

func BenchmarkDecode_U32(b *testing.B) {
	data := bsatn.EncodeU32(42)
	for b.Loop() {
		_, _ = bsatn.DecodeU32(data)
	}
}

func BenchmarkEncode_String_Short(b *testing.B) {
	w := bsatn.NewWriter(32)
	for b.Loop() {
		w.Reset()
		w.PutString("hello")
	}
}

func BenchmarkEncode_String_Medium(b *testing.B) {
	w := bsatn.NewWriter(256)
	s := "the quick brown fox jumps over the lazy dog 0123456789"
	for b.Loop() {
		w.Reset()
		w.PutString(s)
	}
}

func BenchmarkEncode_String_Long(b *testing.B) {
	w := bsatn.NewWriter(4096)
	s := make([]byte, 4000)
	for i := range s {
		s[i] = byte('a' + i%26)
	}
	str := string(s)
	for b.Loop() {
		w.Reset()
		w.PutString(str)
	}
}

func BenchmarkDecode_String_Short(b *testing.B) {
	data := bsatn.EncodeString("hello")
	for b.Loop() {
		_, _ = bsatn.DecodeString(data)
	}
}

func BenchmarkDecode_String_Medium(b *testing.B) {
	data := bsatn.EncodeString("the quick brown fox jumps over the lazy dog 0123456789")
	for b.Loop() {
		_, _ = bsatn.DecodeString(data)
	}
}

func BenchmarkDecode_String_Long(b *testing.B) {
	s := make([]byte, 4000)
	for i := range s {
		s[i] = byte('a' + i%26)
	}
	data := bsatn.EncodeString(string(s))
	for b.Loop() {
		_, _ = bsatn.DecodeString(data)
	}
}

func BenchmarkEncode_EveryPrimitiveStruct(b *testing.B) {
	w := bsatn.NewWriter(256)
	ident := types.NewIdentity([32]byte{1})
	connId := types.NewConnectionId([16]byte{2})
	uuid := types.NewUuid([16]byte{3})
	ts := types.NewTimestamp(1234567890)
	u128 := types.NewUint128(123, 456)
	u256 := types.NewUint256([32]byte{1, 2, 3})
	i128 := types.NewInt128(123, 456)
	i256 := types.NewInt256([32]byte{1, 2, 3})

	for b.Loop() {
		w.Reset()
		w.PutU8(42)
		w.PutU16(1000)
		w.PutU32(100000)
		w.PutU64(1000000000)
		u128.WriteBsatn(w)
		u256.WriteBsatn(w)
		w.PutI8(-42)
		w.PutI16(-1000)
		w.PutI32(-100000)
		w.PutI64(-1000000000)
		i128.WriteBsatn(w)
		i256.WriteBsatn(w)
		w.PutBool(true)
		w.PutF32(3.14)
		w.PutF64(2.71828)
		w.PutString("hello world")
		ident.WriteBsatn(w)
		connId.WriteBsatn(w)
		uuid.WriteBsatn(w)
		ts.WriteBsatn(w)
	}
}

func BenchmarkDecode_EveryPrimitiveStruct(b *testing.B) {
	data := everyPrimitiveData
	for b.Loop() {
		r := bsatn.NewReader(data)
		_, _ = r.GetU8()
		_, _ = r.GetU16()
		_, _ = r.GetU32()
		_, _ = r.GetU64()
		_, _ = types.ReadUint128(r)
		_, _ = types.ReadUint256(r)
		_, _ = r.GetI8()
		_, _ = r.GetI16()
		_, _ = r.GetI32()
		_, _ = r.GetI64()
		_, _ = types.ReadInt128(r)
		_, _ = types.ReadInt256(r)
		_, _ = r.GetBool()
		_, _ = r.GetF32()
		_, _ = r.GetF64()
		_, _ = r.GetString()
		_, _ = types.ReadIdentity(r)
		_, _ = types.ReadConnectionId(r)
		_, _ = types.ReadUuid(r)
		_, _ = types.ReadTimestamp(r)
	}
}

func BenchmarkEncode_LargeTable(b *testing.B) {
	w := bsatn.NewWriter(512)
	ident := types.NewIdentity([32]byte{1})
	connId := types.NewConnectionId([16]byte{2})
	uuid := types.NewUuid([16]byte{3})
	ts := types.NewTimestamp(1234567890)
	u128 := types.NewUint128(123, 456)
	u256 := types.NewUint256([32]byte{1, 2, 3})
	i128 := types.NewInt128(123, 456)
	i256 := types.NewInt256([32]byte{1, 2, 3})

	for b.Loop() {
		w.Reset()
		// Simulate 30+ fields of mixed types
		for range 5 {
			w.PutU8(42)
			w.PutU32(100000)
			w.PutI64(-1000000000)
			w.PutF64(2.71828)
			w.PutString("bench test string")
			w.PutBool(true)
		}
		u128.WriteBsatn(w)
		u256.WriteBsatn(w)
		i128.WriteBsatn(w)
		i256.WriteBsatn(w)
		ident.WriteBsatn(w)
		connId.WriteBsatn(w)
		uuid.WriteBsatn(w)
		ts.WriteBsatn(w)
	}
}

func BenchmarkDecode_LargeTable(b *testing.B) {
	w := bsatn.NewWriter(512)
	for range 5 {
		w.PutU8(42)
		w.PutU32(100000)
		w.PutI64(-1000000000)
		w.PutF64(2.71828)
		w.PutString("bench test string")
		w.PutBool(true)
	}
	types.NewUint128(123, 456).WriteBsatn(w)
	types.NewUint256([32]byte{1, 2, 3}).WriteBsatn(w)
	types.NewInt128(123, 456).WriteBsatn(w)
	types.NewInt256([32]byte{1, 2, 3}).WriteBsatn(w)
	types.NewIdentity([32]byte{1}).WriteBsatn(w)
	types.NewConnectionId([16]byte{2}).WriteBsatn(w)
	types.NewUuid([16]byte{3}).WriteBsatn(w)
	types.NewTimestamp(1234567890).WriteBsatn(w)
	data := w.Bytes()

	for b.Loop() {
		r := bsatn.NewReader(data)
		for range 5 {
			_, _ = r.GetU8()
			_, _ = r.GetU32()
			_, _ = r.GetI64()
			_, _ = r.GetF64()
			_, _ = r.GetString()
			_, _ = r.GetBool()
		}
		_, _ = types.ReadUint128(r)
		_, _ = types.ReadUint256(r)
		_, _ = types.ReadInt128(r)
		_, _ = types.ReadInt256(r)
		_, _ = types.ReadIdentity(r)
		_, _ = types.ReadConnectionId(r)
		_, _ = types.ReadUuid(r)
		_, _ = types.ReadTimestamp(r)
	}
}
