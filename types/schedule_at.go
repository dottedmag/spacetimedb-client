package types

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// ScheduleAt is a sum type representing either a recurring interval or a
// specific point in time, used for scheduled reducer execution.
//
// Variants:
//   - Tag 0: [ScheduleAtInterval] wraps a [TimeDuration] for recurring execution
//   - Tag 1: [ScheduleAtTime] wraps a [Timestamp] for one-time execution
type ScheduleAt interface {
	bsatn.Serializable
	isScheduleAt()
}

// ScheduleAtInterval is the Interval variant of [ScheduleAt] (tag 0).
// It wraps a [TimeDuration] specifying the recurring interval.
type ScheduleAtInterval struct {
	Value TimeDuration
}

func (ScheduleAtInterval) isScheduleAt() {}

func (s ScheduleAtInterval) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(0)
	s.Value.WriteBsatn(w)
}

// ScheduleAtTime is the Time variant of [ScheduleAt] (tag 1).
// It wraps a [Timestamp] specifying the exact execution time.
type ScheduleAtTime struct {
	Value Timestamp
}

func (ScheduleAtTime) isScheduleAt() {}

func (s ScheduleAtTime) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(1)
	s.Value.WriteBsatn(w)
}

// ReadScheduleAt reads a ScheduleAt from a BSATN reader.
func ReadScheduleAt(r bsatn.Reader) (ScheduleAt, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		v, err := ReadTimeDuration(r)
		if err != nil {
			return nil, err
		}
		return ScheduleAtInterval{Value: v}, nil
	case 1:
		v, err := ReadTimestamp(r)
		if err != nil {
			return nil, err
		}
		return ScheduleAtTime{Value: v}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "ScheduleAt"}
	}
}
