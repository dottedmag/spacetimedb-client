package harness

// ModuleHandle represents a published SpacetimeDB module.
type ModuleHandle interface {
	DatabaseName() string
	Variant() string
}

type moduleHandle struct {
	databaseName string
	variant      string
}

func (m *moduleHandle) DatabaseName() string {
	return m.databaseName
}

func (m *moduleHandle) Variant() string {
	return m.variant
}

// Compile-time interface check.
var _ ModuleHandle = (*moduleHandle)(nil)
