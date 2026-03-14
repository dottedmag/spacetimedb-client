package bsatn

import "fmt"

// ErrBufferTooShort is returned by [Reader] methods when the remaining
// bytes in the buffer are fewer than required for the requested type.
type ErrBufferTooShort struct {
	ForType  string
	Expected int
	Given    int
}

func (e *ErrBufferTooShort) Error() string {
	return fmt.Sprintf("bsatn: buffer too short for %s: expected %d bytes, got %d", e.ForType, e.Expected, e.Given)
}

// ErrInvalidBool is returned by [Reader.GetBool] when the byte value
// is not 0x00 (false) or 0x01 (true).
type ErrInvalidBool struct {
	Value uint8
}

func (e *ErrInvalidBool) Error() string {
	return fmt.Sprintf("bsatn: invalid bool value: 0x%02x", e.Value)
}

// ErrInvalidTag is returned when a sum type variant tag read by
// [Reader.GetSumTag] is outside the range of known variants.
// SumName identifies which sum type was being decoded.
type ErrInvalidTag struct {
	Tag     uint8
	SumName string
}

func (e *ErrInvalidTag) Error() string {
	return fmt.Sprintf("bsatn: invalid tag %d for sum type %s", e.Tag, e.SumName)
}
