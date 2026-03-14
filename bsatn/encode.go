package bsatn

// Encode serializes a [Serializable] value to BSATN bytes.
// It allocates a new [Writer], calls v.WriteBsatn, and returns the result.
func Encode(v Serializable) []byte {
	w := NewWriter(64)
	v.WriteBsatn(w)
	return w.Bytes()
}

// Decode deserializes BSATN bytes into a value of type T using the provided
// read function. The read function should consume exactly the bytes needed
// for one value of type T from the [Reader].
func Decode[T any](data []byte, readFn func(Reader) (T, error)) (T, error) {
	r := NewReader(data)
	return readFn(r)
}
