package protocol

// This file is intentionally minimal.
// In the v2 protocol, a reducer call's result comes back as a ReducerResult
// (tag 6) carrying a ReducerOutcome, and the caller's row changes are embedded
// in ReducerOk.TransactionUpdate. Broadcast row changes arrive as a standalone
// TransactionUpdate (tag 4). See server_message.go and encoding.go.
