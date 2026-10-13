package harness

import (
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRustFixtureIsolatedWithoutChangingCheckout(t *testing.T) {
	source := t.TempDir()
	manifest := []byte("edition.workspace = true\n")
	require.NoError(t, os.WriteFile(filepath.Join(source, "Cargo.toml"), manifest, 0600))
	require.NoError(t, os.Mkdir(filepath.Join(source, "src"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "src", "lib.rs"), []byte("pub fn test() {}"), 0600))
	h := &serverHarness{}
	dir, err := h.standaloneRustFixture(source, "2.11.0")
	require.NoError(t, err)
	defer func() { require.NoError(t, h.Cleanup(context.Background())) }()
	contents, err := os.ReadFile(filepath.Join(dir, "Cargo.toml"))
	require.NoError(t, err)
	require.Contains(t, string(contents), `version = "=2.11.0"`)
	require.Contains(t, string(contents), "[workspace]")
	require.False(t, strings.HasPrefix(dir, source+string(os.PathSeparator)))
	original, err := os.ReadFile(filepath.Join(source, "Cargo.toml"))
	require.NoError(t, err)
	require.Equal(t, manifest, original)
	_, err = os.Stat(filepath.Join(dir, "src", "lib.rs"))
	require.NoError(t, err)
}
