package client

import (
	"testing"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// A failed reducer's payload is the module's own error text, written through the
// host as a BSATN string. Decoding it is what lets a caller distinguish "that
// name is taken" from "you are going too fast".
func TestReducerErrMessageDecodesBSATNString(t *testing.T) {
	got := reducerErrMessage(bsatn.EncodeString("conflict"))
	if got != "conflict" {
		t.Fatalf("reducerErrMessage = %q, want %q", got, "conflict")
	}
}

// A host that writes the message as plain bytes must keep working.
func TestReducerErrMessageAcceptsRawBytes(t *testing.T) {
	got := reducerErrMessage([]byte("boom"))
	if got != "boom" {
		t.Fatalf("reducerErrMessage = %q, want %q", got, "boom")
	}
}

// A reducer that failed without saying why still yields something showable.
func TestReducerErrMessageFallsBackWhenEmpty(t *testing.T) {
	for name, payload := range map[string][]byte{
		"nil":          nil,
		"empty":        {},
		"empty string": bsatn.EncodeString(""),
		"blank string": bsatn.EncodeString("   "),
	} {
		if got := reducerErrMessage(payload); got != "reducer returned an error" {
			t.Errorf("%s: reducerErrMessage = %q, want the fallback", name, got)
		}
	}
}
