package protocol

// This file is intentionally minimal for v1 protocol.
// In v1, reducer results come as TransactionUpdate (tag 1) or TransactionUpdateLight (tag 2)
// server messages rather than as a separate ReducerResult message type.
