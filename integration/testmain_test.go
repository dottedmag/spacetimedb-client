//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.digitalxero.dev/spacetimedb-client/integration/harness"
)

var (
	testHarness   harness.ServerHarness
	moduleHandles map[string]harness.ModuleHandle
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	uri := os.Getenv("SPACETIMEDB_URI")
	if uri == "" {
		uri = "ws://localhost:3000"
	}

	modulesPath := os.Getenv("SPACETIMEDB_MODULES_PATH")
	repoRef := os.Getenv("SPACETIMEDB_REPO_REF")
	if repoRef == "" {
		repoRef = "v2.11.0"
	}

	modulesEnv := os.Getenv("SPACETIMEDB_MODULES")
	variants := harness.ParseModuleVariants(modulesEnv)

	builder := harness.NewServerHarness().
		WithUri(uri).
		WithRepoRef(repoRef).
		WithModuleVariants(variants...)

	if modulesPath != "" {
		builder = builder.WithModulesPath(modulesPath)
	}

	h, err := builder.Build(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: harness setup failed: %v\n", err)
		os.Exit(1)
	}
	testHarness = h

	moduleHandles = make(map[string]harness.ModuleHandle, len(variants))
	for _, variant := range variants {
		handle, err := h.PublishModule(ctx, variant)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: failed to publish %s module: %v\n", variant, err)
			_ = h.Cleanup(ctx)
			os.Exit(1)
		}
		moduleHandles[variant] = handle
		fmt.Printf("Published %s module as %s\n", variant, handle.DatabaseName())
	}

	code := m.Run()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	if err := h.Cleanup(cleanupCtx); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: cleanup failed: %v\n", err)
	}

	os.Exit(code)
}
