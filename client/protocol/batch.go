package protocol

import "go.digitalxero.dev/spacetimedb-client/bsatn"

// SubscribeBatch adds multiple independent query sets at one transaction offset.
// It is client message tag 5 in SpacetimeDB 2.11's v2/v3 schema.
type SubscribeBatch struct {
	RequestID uint32
	Sets      []SubscribeSet
}
type SubscribeSet struct {
	QuerySetID   uint32
	QueryStrings []string
}

func (*SubscribeBatch) clientMessageTag() uint8 { return 5 }
func (m *SubscribeBatch) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(5)
	w.PutU32(m.RequestID)
	w.PutArrayLen(uint32(len(m.Sets)))
	for _, set := range m.Sets {
		w.PutU32(set.QuerySetID)
		w.PutArrayLen(uint32(len(set.QueryStrings)))
		for _, query := range set.QueryStrings {
			w.PutString(query)
		}
	}
}

// SubscribeBatchApplied reports an independent outcome for each requested set.
type SubscribeBatchApplied struct {
	RequestID uint32
	Results   []SubscribeSetResult
}
type SubscribeSetResult struct {
	QuerySetID uint32
	Rows       *QueryRows
	Error      *string
}

func (*SubscribeBatchApplied) serverMessageTag() uint8 { return 8 }

func readSubscribeBatchApplied(r bsatn.Reader) (ServerMessage, error) {
	id, err := r.GetU32()
	if err != nil {
		return nil, err
	}
	count, err := r.GetArrayLen()
	if err != nil {
		return nil, err
	}
	if err := checkArrayLength(r, count, 9); err != nil {
		return nil, err
	}
	results := make([]SubscribeSetResult, count)
	for i := range results {
		setID, err := r.GetU32()
		if err != nil {
			return nil, err
		}
		tag, err := r.GetSumTag()
		if err != nil {
			return nil, err
		}
		results[i].QuerySetID = setID
		switch tag {
		case 0:
			rows, err := readQueryRows(r)
			if err != nil {
				return nil, err
			}
			results[i].Rows = &rows
		case 1:
			message, err := r.GetString()
			if err != nil {
				return nil, err
			}
			results[i].Error = &message
		default:
			return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "SubscribeSetOutcome"}
		}
	}
	return &SubscribeBatchApplied{RequestID: id, Results: results}, nil
}
