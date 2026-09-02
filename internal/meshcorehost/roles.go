package meshcorehost

import "meshrunner.dev/pkg/meshcore"

// The role words, spelled once — RoleName and RoleByte are the two
// directions of the same dictionary.
const (
	RoleAdmin     = "admin"
	RoleReadWrite = "read-write"
	RoleReadOnly  = "read-only"
	RoleGuest     = "guest"
)

// RoleByte is RoleName backwards: the byte a role's word means, for
// the channels that speak words. ok is false for a word no role
// carries.
func RoleByte(name string) (byte, bool) {
	switch name {
	case RoleAdmin:
		return meshcore.PermAdmin, true
	case RoleReadWrite:
		return meshcore.PermReadWrite, true
	case RoleReadOnly:
		return meshcore.PermReadOnly, true
	case RoleGuest:
		return meshcore.PermGuest, true
	}
	return 0, false
}

// RoleName names the role a permission byte carries — the reference's
// four, by the low two bits. The one place the words exist.
func RoleName(perms byte) string {
	switch meshcore.Role(perms) {
	case meshcore.PermAdmin:
		return RoleAdmin
	case meshcore.PermReadWrite:
		return RoleReadWrite
	case meshcore.PermReadOnly:
		return RoleReadOnly
	default:
		return RoleGuest
	}
}
