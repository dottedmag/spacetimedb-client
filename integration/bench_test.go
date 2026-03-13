//go:build integration

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.digitalxero.dev/spacetimedb-client/client"
	"go.digitalxero.dev/spacetimedb-client/integration/harness"
	"go.digitalxero.dev/spacetimedb-client/integration/schema"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func benchModuleVariants() []string {
	env := os.Getenv("BENCH_MODULES")
	if env == "" {
		env = "rust"
	}
	return harness.ParseModuleVariants(env)
}

func benchConnect(b *testing.B, dbName string) client.DbConnection {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	connected := make(chan struct{}, 1)
	builder := testHarness.NewConnection(dbName).
		OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		})

	conn, err := builder.Build(ctx)
	require.NoError(b, err)

	runDone := make(chan error, 1)
	go func() {
		runDone <- conn.Run(ctx)
	}()

	select {
	case <-connected:
	case err := <-runDone:
		cancel()
		require.NoError(b, err)
	case <-time.After(defaultTimeout):
		cancel()
		require.Fail(b, "timed out waiting for connect")
	}

	b.Cleanup(func() {
		_ = conn.Disconnect()
		cancel()
		<-runDone
	})

	return conn
}

func BenchmarkConnect(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			for b.Loop() {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				connected := make(chan struct{}, 1)

				conn, err := testHarness.NewConnection(h.DatabaseName()).
					OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
						close(connected)
					}).
					Build(ctx)
				if err != nil {
					cancel()
					b.Fatal(err)
				}

				runDone := make(chan error, 1)
				go func() { runDone <- conn.Run(ctx) }()

				select {
				case <-connected:
				case <-time.After(defaultTimeout):
					cancel()
					b.Fatal("timeout")
				}

				_ = conn.Disconnect()
				cancel()
				<-runDone
			}
		})
	}
}

func BenchmarkConnect_WithToken(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			// Get a token first
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			var token string
			connected := make(chan struct{}, 1)

			conn, err := testHarness.NewConnection(h.DatabaseName()).
				OnConnect(func(_ client.DbConnection, _ types.Identity, t string) {
					token = t
					close(connected)
				}).
				Build(ctx)
			require.NoError(b, err)

			runDone := make(chan error, 1)
			go func() { runDone <- conn.Run(ctx) }()

			<-connected
			_ = conn.Disconnect()
			cancel()
			<-runDone

			b.ResetTimer()
			for b.Loop() {
				ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTimeout)
				connected2 := make(chan struct{}, 1)

				conn2, err := testHarness.NewConnection(h.DatabaseName()).
					WithToken(token).
					OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
						close(connected2)
					}).
					Build(ctx2)
				if err != nil {
					cancel2()
					b.Fatal(err)
				}

				runDone2 := make(chan error, 1)
				go func() { runDone2 <- conn2.Run(ctx2) }()

				<-connected2
				_ = conn2.Disconnect()
				cancel2()
				<-runDone2
			}
		})
	}
}

func BenchmarkReducer_InsertU32(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			conn := benchConnect(b, h.DatabaseName())
			conn.RegisterTable(schema.OneU32Def)

			b.ResetTimer()
			for i := range b.N {
				_ = conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(uint32(i)))
			}
		})
	}
}

func BenchmarkReducer_InsertString(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			conn := benchConnect(b, h.DatabaseName())
			conn.RegisterTable(schema.OneStringDef)

			b.ResetTimer()
			for range b.N {
				_ = conn.CallReducer("insert_one_string", schema.InsertOneStringArgs("benchmark test string"))
			}
		})
	}
}

func BenchmarkSubscribe_EmptyTable(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			for b.Loop() {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				connected := make(chan struct{}, 1)
				applied := make(chan struct{}, 1)

				conn, err := testHarness.NewConnection(h.DatabaseName()).
					OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
						close(connected)
					}).
					Build(ctx)
				if err != nil {
					cancel()
					b.Fatal(err)
				}

				conn.RegisterTable(schema.OneU32Def)

				runDone := make(chan error, 1)
				go func() { runDone <- conn.Run(ctx) }()

				<-connected

				_, err = conn.Subscribe("SELECT * FROM one_u_32").
					OnApplied(func() { close(applied) }).
					Build()
				if err != nil {
					cancel()
					b.Fatal(err)
				}

				<-applied
				_ = conn.Disconnect()
				cancel()
				<-runDone
			}
		})
	}
}

func BenchmarkSubscribe_1000Rows(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			// Pre-populate 1000 rows
			conn := benchConnect(b, h.DatabaseName())
			conn.RegisterTable(schema.OneU32Def)

			for i := range 1000 {
				_ = conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(uint32(i)))
			}
			// Wait a bit for rows to be committed
			time.Sleep(2 * time.Second)
			_ = conn.Disconnect()
			time.Sleep(500 * time.Millisecond)

			b.ResetTimer()
			for b.Loop() {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				connected := make(chan struct{}, 1)
				applied := make(chan struct{}, 1)

				conn2, err := testHarness.NewConnection(h.DatabaseName()).
					OnConnect(func(_ client.DbConnection, _ types.Identity, _ string) {
						close(connected)
					}).
					Build(ctx)
				if err != nil {
					cancel()
					b.Fatal(err)
				}

				conn2.RegisterTable(schema.OneU32Def)

				runDone := make(chan error, 1)
				go func() { runDone <- conn2.Run(ctx) }()

				<-connected

				_, err = conn2.Subscribe("SELECT * FROM one_u_32").
					OnApplied(func() { close(applied) }).
					Build()
				if err != nil {
					cancel()
					b.Fatal(err)
				}

				<-applied
				_ = conn2.Disconnect()
				cancel()
				<-runDone
			}
		})
	}
}

func BenchmarkE2E_InsertAndObserve(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			conn := benchConnect(b, h.DatabaseName())
			conn.RegisterTable(schema.OneU32Def)

			applied := make(chan struct{}, 1)
			_, err := conn.Subscribe("SELECT * FROM one_u_32").
				OnApplied(func() {
					select {
					case applied <- struct{}{}:
					default:
					}
				}).
				Build()
			require.NoError(b, err)
			<-applied

			tc := conn.Cache().GetTable("one_u_32")
			require.NotNil(b, tc)

			b.ResetTimer()
			for i := range b.N {
				inserted := make(chan struct{}, 1)
				cbID := tc.OnInsert(func(_ any) {
					select {
					case inserted <- struct{}{}:
					default:
					}
				})

				_ = conn.CallReducer("insert_one_u_32", schema.InsertOneU32Args(uint32(i+10000)))

				select {
				case <-inserted:
				case <-time.After(defaultTimeout):
					b.Fatal("timeout waiting for insert callback")
				}
				tc.RemoveCallback(cbID)
			}
		})
	}
}

func BenchmarkOneOffQuery(b *testing.B) {
	for _, variant := range benchModuleVariants() {
		h, ok := moduleHandles[variant]
		if !ok {
			continue
		}
		b.Run(variant, func(b *testing.B) {
			conn := benchConnect(b, h.DatabaseName())

			b.ResetTimer()
			for b.Loop() {
				_, _ = conn.OneOffQuery("SELECT * FROM one_u_32")
			}
		})
	}
}

