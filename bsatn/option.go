package bsatn

// WriteOption writes an Option<T> value as a BSATN sum type.
// A non-nil pointer is encoded as Some: tag 0 followed by the encoded value.
// A nil pointer is encoded as None: tag 1 with no payload.
//
// Wire format:
//
//	Some(v): [0x00] [encoded v]
//	None:    [0x01]
func WriteOption[T Serializable](w Writer, v *T) {
	if v != nil {
		w.PutSumTag(0)
		(*v).WriteBsatn(w)
	} else {
		w.PutSumTag(1)
	}
}

// ReadOption reads a BSATN Option<T> sum type using the provided read
// function. Returns a non-nil pointer for Some (tag 0) or nil for None
// (tag 1). Returns [ErrInvalidTag] for any other tag value.
func ReadOption[T any](r Reader, readFn func(Reader) (T, error)) (*T, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // Some
		v, err := readFn(r)
		if err != nil {
			return nil, err
		}
		return &v, nil
	case 1: // None
		return nil, nil
	default:
		return nil, &ErrInvalidTag{Tag: tag, SumName: "Option"}
	}
}
