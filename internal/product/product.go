// Package product is the single authority on what this software IS:
// its name, its slug, where it lives on the web and where its updates
// come from. Three contracts live here and must not be confused — the
// immutable presentation identity, the install ABI a running fleet
// depends on, and (in internal/version) the variable identity of one
// build. A rebrand may change the first; it must never silently move
// the second.
package product

// The presentation identity: what screens, banners and manifests call
// this software.
const (
	// Slug is the machine name: binaries, artifact prefixes, MQTT
	// client ids, the manifest's product field.
	Slug = "lotor"
	// Name is the human name, capitalised the one official way.
	Name = "Lotor"
	// Description is the one-line pitch every surface repeats.
	Description = "A mesh relay daemon"
	// Homepage is the project's public address.
	Homepage = "https://meshrunner.dev/lotor"
	// UpdateBase is where the update channels are served from.
	UpdateBase = "https://updates.meshrunner.dev/lotor"
)

// The install ABI: where a deployed daemon lives. Deliberately
// spelled out rather than derived from Slug — a rebrand must not
// silently relocate the configuration of a running fleet or rename a
// service systemd already manages.
const (
	// InstallBinary is the path the service unit executes and the
	// self-update replaces.
	InstallBinary = "/usr/local/bin/lotor"
	// InstallStateDir is the state directory: the config store, the
	// journal.
	InstallStateDir = "/var/lib/lotor"
	// InstallService is the systemd unit name, without the suffix.
	InstallService = "lotor"
)
