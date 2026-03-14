package bsatn

// WriteSum writes a sum type (tagged union) value.
//
// Wire format: u8 variant tag followed by the variant payload.
// If payload is nil, only the tag byte is written.
func WriteSum(w Writer, tag uint8, payload Serializable) {
	w.PutSumTag(tag)
	if payload != nil {
		payload.WriteBsatn(w)
	}
}

// WriteSumUnit writes a sum type variant with no payload (unit/empty product).
// This is equivalent to WriteSum(w, tag, nil).
func WriteSumUnit(w Writer, tag uint8) {
	w.PutSumTag(tag)
}
