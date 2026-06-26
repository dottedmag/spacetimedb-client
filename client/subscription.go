package client

// SubscriptionBuilder configures and creates a subscription to one or more
// SQL queries. Create a SubscriptionBuilder by calling [DbConnection.Subscribe]
// with the desired queries.
//
// Use OnApplied to register a callback that fires once the initial matching
// rows have been delivered and applied to the local cache. Use OnError to
// register a callback that fires if the server rejects the subscription. Call
// Build to send the subscription to the server and obtain a
// [SubscriptionHandle].
//
// Each call to Build registers an independent subscription. Building additional
// subscriptions adds to the set of active queries; it does not replace previous
// subscriptions. Use [SubscriptionHandle.Unsubscribe] to remove one.
type SubscriptionBuilder interface {
	OnApplied(fn func()) SubscriptionBuilder
	OnError(fn func(error)) SubscriptionBuilder
	Build() (SubscriptionHandle, error)
}

// SubscriptionHandle represents an active subscription returned by
// [SubscriptionBuilder.Build]. Use Unsubscribe to cancel the subscription,
// IsActive to check whether it is still active, and QueryID to obtain the
// server-side identifier for this subscription.
type SubscriptionHandle interface {
	Unsubscribe() error
	IsActive() bool
	QueryID() uint32
}

// CallbackID is an opaque identifier returned when registering a callback.
// Pass it to RemoveCallback on a [cache.TableCache] to unregister.
type CallbackID uint64
