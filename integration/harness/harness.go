package harness

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.digitalxero.dev/spacetimedb-client/client"
)

// ServerHarnessBuilder configures and creates a ServerHarness.
type ServerHarnessBuilder interface {
	WithUri(uri string) ServerHarnessBuilder
	WithModulesPath(path string) ServerHarnessBuilder
	WithRepoRef(ref string) ServerHarnessBuilder
	WithModuleVariants(variants ...string) ServerHarnessBuilder
	Build(ctx context.Context) (ServerHarness, error)
}

// ServerHarness manages a SpacetimeDB server connection for integration tests.
type ServerHarness interface {
	Uri() string
	ModulesPath() string
	PublishModule(ctx context.Context, variant string) (ModuleHandle, error)
	NewConnection(dbName string) client.DbConnectionBuilder
	Cleanup(ctx context.Context) error
}

// NewServerHarness returns a new ServerHarnessBuilder.
func NewServerHarness() ServerHarnessBuilder {
	return &serverHarness{
		uri:     "ws://localhost:3000",
		repoRef: "v2.12.0",
	}
}

type serverHarness struct {
	uri            string
	modulesPath    string
	repoRef        string
	moduleVariants []string

	// runtime state
	tempDir      string
	ownsTempDir  bool
	publishedDBs []string
	fixtureDirs  []string
}

func (h *serverHarness) WithUri(uri string) ServerHarnessBuilder {
	h.uri = uri
	return h
}

func (h *serverHarness) WithModulesPath(path string) ServerHarnessBuilder {
	h.modulesPath = path
	return h
}

func (h *serverHarness) WithRepoRef(ref string) ServerHarnessBuilder {
	h.repoRef = ref
	return h
}

func (h *serverHarness) WithModuleVariants(variants ...string) ServerHarnessBuilder {
	h.moduleVariants = variants
	return h
}

func (h *serverHarness) Build(ctx context.Context) (ServerHarness, error) {
	if err := h.verifyServer(ctx); err != nil {
		return nil, fmt.Errorf("server not reachable at %s: %w", h.uri, err)
	}

	if h.modulesPath == "" {
		tmpDir, err := h.cloneModules(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to clone SpacetimeDB modules: %w", err)
		}
		h.modulesPath = filepath.Join(tmpDir, "modules")
		h.tempDir = tmpDir
		h.ownsTempDir = true
	}

	return h, nil
}

func (h *serverHarness) Uri() string {
	return h.uri
}

func (h *serverHarness) ModulesPath() string {
	return h.modulesPath
}

func (h *serverHarness) PublishModule(ctx context.Context, variant string) (ModuleHandle, error) {
	subdir, ok := variantSubdir(variant)
	if !ok {
		return nil, fmt.Errorf("unknown module variant: %s", variant)
	}

	projectPath := filepath.Join(h.modulesPath, subdir)
	if _, err := os.Stat(projectPath); err != nil {
		return nil, fmt.Errorf("module path not found: %s: %w", projectPath, err)
	}
	if variant == "rust" && h.ownsTempDir && os.Getenv("SPACETIMEDB_WORKSPACE_SDK") != "true" {
		version := strings.TrimPrefix(h.repoRef, "v")
		if regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(version) {
			standalone, err := h.standaloneRustFixture(projectPath, version)
			if err != nil {
				return nil, err
			}
			projectPath = standalone
		}
	}

	dbName := fmt.Sprintf("go-integ-%s-%d", variant, time.Now().UnixNano())

	httpURI, err := wsToHTTP(h.uri)
	if err != nil {
		return nil, fmt.Errorf("failed to convert URI: %w", err)
	}

	cmd := exec.CommandContext(ctx, "spacetime", "publish",
		"--module-path", projectPath,
		"--server", httpURI,
		"--delete-data",
		"--yes",
		dbName,
	)
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = ensureCargoInPath(append(os.Environ(), "NO_COLOR=1"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("spacetime publish failed: %w\noutput: %s", err, string(output))
	}

	h.publishedDBs = append(h.publishedDBs, dbName)

	return &moduleHandle{
		databaseName: dbName,
		variant:      variant,
	}, nil
}

func (h *serverHarness) NewConnection(dbName string) client.DbConnectionBuilder {
	return client.NewDbConnection().
		WithUri(h.uri).
		WithDatabaseName(dbName)
}

func (h *serverHarness) Cleanup(ctx context.Context) error {
	httpURI, _ := wsToHTTP(h.uri)

	var errs []error
	for _, db := range h.publishedDBs {
		cmd := exec.CommandContext(ctx, "spacetime", "delete", db, "--server", httpURI)
		cmd.WaitDelay = 5 * time.Second
		cmd.Env = ensureCargoInPath(append(os.Environ(), "NO_COLOR=1"))
		if output, err := cmd.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w\noutput: %s", db, err, string(output)))
		}
	}

	if h.ownsTempDir && h.tempDir != "" {
		if err := os.RemoveAll(h.tempDir); err != nil {
			errs = append(errs, fmt.Errorf("remove temp dir: %w", err))
		}
	}
	for _, dir := range h.fixtureDirs {
		if err := os.RemoveAll(dir); err != nil {
			errs = append(errs, fmt.Errorf("remove fixture dir: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

func (h *serverHarness) verifyServer(ctx context.Context) error {
	httpURI, err := wsToHTTP(h.uri)
	if err != nil {
		return err
	}

	healthURL := httpURI + "/v1/ping"

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, healthURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned status %d", resp.StatusCode)
	}
	return nil
}

func (h *serverHarness) cloneModules(ctx context.Context) (string, error) {
	tmpDir, err := os.MkdirTemp("", "spacetimedb-modules-*")
	if err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, "git", "clone",
		"--depth", "1",
		"--branch", h.repoRef,
		"https://github.com/clockworklabs/SpacetimeDB.git",
		tmpDir,
	)
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = ensureCargoInPath(append(os.Environ(), "NO_COLOR=1"))
	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", fmt.Errorf("git clone failed: %w\noutput: %s", err, string(output))
	}

	return tmpDir, nil
}

// standaloneRustFixture excludes unrelated upstream workspace dependencies.
func (h *serverHarness) standaloneRustFixture(source, version string) (string, error) {
	dir, err := os.MkdirTemp("", "spacetimedb-rust-fixture-*")
	if err != nil {
		return "", err
	}
	if err := os.CopyFS(dir, os.DirFS(source)); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("copy Rust fixture: %w", err)
	}
	manifest := fmt.Sprintf(`[package]
name = "sdk-test-module"
version = "0.1.0"
edition = "2024"

[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
anyhow = "1.0.68"
paste = "1.0"
spacetimedb = { version = "=%s", features = ["unstable"] }
`, version)
	if err := os.WriteFile(filepath.Join(dir, "Cargo.toml"), []byte(manifest), 0600); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	h.fixtureDirs = append(h.fixtureDirs, dir)
	return dir, nil
}

// variantSubdir returns the module subdirectory for a variant name.
func variantSubdir(variant string) (string, bool) {
	switch strings.ToLower(variant) {
	case "rust":
		return "sdk-test", true
	case "typescript":
		return "sdk-test-ts", true
	case "cpp":
		return "sdk-test-cpp", true
	default:
		return "", false
	}
}

// wsToHTTP converts a WebSocket URI to an HTTP URI.
func wsToHTTP(wsURI string) (string, error) {
	u, err := url.Parse(wsURI)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		// assume it's already http/https
	}
	return u.String(), nil
}

// ParseModuleVariants parses a comma-separated module variant string.
// "all" expands to ["rust", "typescript", "cpp"].
func ParseModuleVariants(s string) []string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return []string{"rust"}
	}
	if s == "all" {
		return []string{"rust", "typescript", "cpp"}
	}
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// ensureCargoInPath adds ~/.cargo/bin to PATH if not already present.
// This ensures child processes (like spacetime publish) can find rustup/cargo.
func ensureCargoInPath(env []string) []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return env
	}
	cargoBin := filepath.Join(home, ".cargo", "bin")

	for i, e := range env {
		if strings.HasPrefix(e, "PATH=") {
			if !strings.Contains(e, cargoBin) {
				env[i] = e + string(os.PathListSeparator) + cargoBin
			}
			return env
		}
	}
	return append(env, "PATH="+cargoBin)
}

// Compile-time interface checks.
var (
	_ ServerHarnessBuilder = (*serverHarness)(nil)
	_ ServerHarness        = (*serverHarness)(nil)
)
