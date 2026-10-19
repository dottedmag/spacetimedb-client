package client

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"go.digitalxero.dev/spacetimedb-client/types"
)

// ManagedSubscription is one session owned by RunSubscription. Register tables
// in Initialize, before the event loop starts. A fresh session has a fresh cache.
type ManagedSubscription interface {
	DbConnection
	// Ready reports whether the complete subscription is available. It is false
	// before initial application, during reconnect, and after any session failure.
	// Hold the mutex shared with the transaction hooks across Ready and cache reads.
	Ready() bool
	// Changes coalesces completed cache batches and readiness changes. Notifications
	// are sent after OnTxFinish. The channel is not closed; observe the session context.
	Changes() <-chan struct{}
}

// ManagedSubscriptionOptions describes one query set and its local consumers.
type ManagedSubscriptionOptions struct {
	Queries []string
	// Initialize runs once per fresh connection, before receiving any rows. It
	// registers cache definitions and may publish the session to other goroutines.
	Initialize func(ManagedSubscription)
	// Observe optionally runs alongside the event loop after initial application.
	// It may perform network I/O. Returning nil finishes RunSubscription; returning
	// an error restarts the session. It must stop when its session context ends.
	Observe func(context.Context, ManagedSubscription) error
}

type managedSubscription struct {
	DbConnection
	ready   atomic.Bool
	changes chan struct{}
}

func (s *managedSubscription) Ready() bool              { return s.ready.Load() }
func (s *managedSubscription) Changes() <-chan struct{} { return s.changes }

func (s *managedSubscription) notify() {
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func (b *dbConnectionBuilder) RunSubscription(ctx context.Context, options ManagedSubscriptionOptions) error {
	if len(options.Queries) == 0 {
		return &SubscriptionError{Message: "at least one query is required"}
	}
	settings := *b
	b = &settings
	options.Queries = append([]string(nil), options.Queries...)
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		finished, err := b.runSubscription(ctx, options)
		if ctx.Err() != nil || finished {
			return nil
		}
		if err != nil {
			// Transport errors can contain credentials in URLs.
			slog.WarnContext(ctx, "subscription interrupted; retrying")
		}
		if time.Since(started) >= time.Minute {
			backoff = time.Second
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
	return nil
}

func (b *dbConnectionBuilder) runSubscription(ctx context.Context, options ManagedSubscriptionOptions) (bool, error) {
	// Bound reconnect dials too: they inherit the event loop's context.
	ctx, cancel := context.WithTimeout(ctx, 24*time.Hour)
	defer cancel()
	session := &managedSubscription{changes: make(chan struct{}, 1)}
	initial := make(chan struct{}, 1)
	applied := false
	var subscriptionErr error
	var applyTimer *time.Timer
	settings := *b
	settings.automaticReconnect = true
	settings.onTxFinish = func(err error) {
		session.ready.Store(applied && err == nil)
		if session.Ready() {
			applyTimer.Stop()
		}
		if b.onTxFinish != nil {
			b.onTxFinish(err)
		}
		session.notify()
		if session.Ready() {
			select {
			case initial <- struct{}{}:
			default:
			}
		}
	}
	settings.onDisconnect = func(conn DbConnection, err error) {
		// Lifecycle invalidation uses the same boundary as cache application, so
		// an external reader cannot consume a retired session under its mutex.
		if b.onTxStart != nil {
			b.onTxStart()
		}
		applied = false
		session.ready.Store(false)
		if b.onTxFinish != nil {
			b.onTxFinish(err)
		}
		session.notify()
		applyTimer.Stop()
		if b.onDisconnect != nil {
			b.onDisconnect(conn, err)
		}
	}
	settings.onConnect = func(conn DbConnection, identity types.Identity, token string) {
		applyTimer.Reset(10 * time.Second)
		if b.onConnect != nil {
			b.onConnect(conn, identity, token)
		}
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	conn, err := settings.Build(connectCtx)
	cancelConnect()
	if err != nil {
		return false, err
	}
	session.DbConnection = conn
	applyTimer = time.AfterFunc(10*time.Second, cancel)
	defer applyTimer.Stop()
	defer conn.Disconnect()
	if options.Initialize != nil {
		options.Initialize(session)
	}
	if _, err := conn.Subscribe(options.Queries...).OnApplied(func() {
		applied = true
	}).OnError(func(err error) {
		applied = false
		subscriptionErr = err
		cancel()
	}).Build(); err != nil {
		return false, err
	}
	consumer := make(chan error, 1)
	go func() {
		var err error
		select {
		case <-initial:
			if options.Observe != nil {
				err = options.Observe(ctx, session)
			} else {
				<-ctx.Done()
				err = ctx.Err()
			}
		case <-ctx.Done():
			err = ctx.Err()
		}
		consumer <- err
		cancel()
	}()
	err = conn.Run(ctx)
	cancel()
	consumerErr := <-consumer
	return consumerErr == nil, errors.Join(err, consumerErr, subscriptionErr)
}
