package terminal

// Role names the meaning of one flat, sanitized, cell-clipped text span.
type Role string

const (
	RoleText      Role = "text"
	RoleMuted     Role = "muted"
	RoleBorder    Role = "border"
	RoleSelection Role = "selection"
	RoleAccent    Role = "accent"
	RoleSuccess   Role = "success"
	RoleWarning   Role = "warning"
	RoleError     Role = "error"
	RoleInfo      Role = "info"
	RoleKey       Role = "key"
)

// Styler formats trusted renderer spans using an immutable palette. Callers
// treat a nil Styler as identity. Implementations must restore base attributes
// after each span and must not change terminal-emulator palette settings.
// Secret input and machine JSON never pass through this presentation boundary.
type Styler interface {
	Paint(Role, string) string
}
