package meshcore

// The client table and its roles live in the shared server kernel,
// meshrunner.dev/lotor/internal/meshcorehost — the reference builds
// its repeater and its room server on one ClientACL, and so does this
// daemon. This engine keeps its own spellings for the kernel's names,
// so the code that judges frames reads as it always did, and exports
// the handful the console and the store speak.

import (
	"meshrunner.dev/lotor/internal/meshcorehost"
)

// The kernel's names, re-exported for the daemon's store and console.
type (
	// PersistedSession is one durable access entry as a store keeps it.
	PersistedSession = meshcorehost.PersistedSession
	// SessionStore persists the access entries in the client table.
	SessionStore = meshcorehost.SessionStore
	// ACLEntry is one durable authorisation, as the console shows it.
	ACLEntry = meshcorehost.ACLEntry
	// ClientSession is one logged-in companion, as an operator sees it.
	ClientSession = meshcorehost.ClientSession

	client      = meshcorehost.Client
	outPath     = meshcorehost.OutPath
	acl         = meshcorehost.Table
	rateLimiter = meshcorehost.RateLimiter
)

// The role words and their bytes, the kernel's dictionary.
const (
	RoleAdmin     = meshcorehost.RoleAdmin
	RoleReadWrite = meshcorehost.RoleReadWrite
	RoleReadOnly  = meshcorehost.RoleReadOnly
	RoleGuest     = meshcorehost.RoleGuest
)

var (
	// ErrNoSuchEntry says a removal named nobody the table holds.
	ErrNoSuchEntry = meshcorehost.ErrNoSuchEntry
	// ErrNoSuchSession says a close named no currently active session.
	ErrNoSuchSession = meshcorehost.ErrNoSuchSession
	errSessionsFull  = meshcorehost.ErrSessionsFull
)

// RoleByte is RoleName backwards: the byte a role's word means.
func RoleByte(name string) (byte, bool) { return meshcorehost.RoleByte(name) }

// RoleName names the role a permission byte carries.
func RoleName(perms byte) string { return meshcorehost.RoleName(perms) }

// newACL makes this engine's table at the repeater's capacity.
func newACL(store SessionStore) *acl { return meshcorehost.NewTable(store, maxClients) }
