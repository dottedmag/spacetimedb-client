package bsatn

// WriteMap writes a sequence of key-value pairs as a BSATN map.
//
// Wire format: u32 LE pair count followed by each key-value pair encoded
// sequentially. The items parameter is a slice of structs rather than a
// Go map to ensure deterministic encoding order.
func WriteMap[K Serializable, V Serializable](w Writer, items []struct {
	Key   K
	Value V
}) {
	w.PutMapLen(uint32(len(items)))
	for _, item := range items {
		item.Key.WriteBsatn(w)
		item.Value.WriteBsatn(w)
	}
}

// ReadMap reads a BSATN map using the provided key and value read functions.
// It reads a u32 LE count, then decodes that many key-value pairs.
// Keys must be comparable for use as Go map keys.
func ReadMap[K comparable, V any](r Reader, readK func(Reader) (K, error), readV func(Reader) (V, error)) (map[K]V, error) {
	count, err := r.GetMapLen()
	if err != nil {
		return nil, err
	}
	m := make(map[K]V, min(count, 1024))
	for i := uint32(0); i < count; i++ {
		k, err := readK(r)
		if err != nil {
			return nil, err
		}
		v, err := readV(r)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
