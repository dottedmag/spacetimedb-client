package cache

// TypedTableCache wraps a TableCache with compile-time type safety.
// It delegates to the underlying untyped TableCache, performing type
// assertions at the boundary.
type TypedTableCache[T any] struct {
	inner TableCache
}

// NewTypedTableCache creates a new TypedTableCache wrapping the given TableCache.
func NewTypedTableCache[T any](inner TableCache) *TypedTableCache[T] {
	return &TypedTableCache[T]{inner: inner}
}

// Count returns the number of rows in the cache.
func (tc *TypedTableCache[T]) Count() int {
	return tc.inner.Count()
}

// Iter iterates over all rows in the cache with type safety.
// Return false from fn to stop iteration.
func (tc *TypedTableCache[T]) Iter(fn func(row T) bool) {
	tc.inner.Iter(func(row any) bool {
		return fn(row.(T))
	})
}

// OnInsert registers a typed callback for row insertions.
func (tc *TypedTableCache[T]) OnInsert(fn TypedInsertCallback[T]) CallbackID {
	return tc.inner.OnInsert(func(row any) {
		fn(row.(T))
	})
}

// OnDelete registers a typed callback for row deletions.
func (tc *TypedTableCache[T]) OnDelete(fn TypedDeleteCallback[T]) CallbackID {
	return tc.inner.OnDelete(func(row any) {
		fn(row.(T))
	})
}

// OnUpdate registers a typed callback for row updates.
func (tc *TypedTableCache[T]) OnUpdate(fn TypedUpdateCallback[T]) CallbackID {
	return tc.inner.OnUpdate(func(old, new any) {
		fn(old.(T), new.(T))
	})
}

// OnEvent registers a typed callback for ephemeral event-table rows. Event rows
// fire only this callback and are not retained in the cache.
func (tc *TypedTableCache[T]) OnEvent(fn TypedEventCallback[T]) CallbackID {
	return tc.inner.OnEvent(func(row any) {
		fn(row.(T))
	})
}

// RemoveCallback removes a previously registered callback by its ID.
func (tc *TypedTableCache[T]) RemoveCallback(id CallbackID) {
	tc.inner.RemoveCallback(id)
}
