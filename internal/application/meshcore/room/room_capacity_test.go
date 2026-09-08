package room

import (
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/meshcorehost"
	mesh "meshrunner.dev/pkg/meshcore"
)

// The database returns newest first. A reader therefore precedes an
// older administrator in the very case where shrinking used to hide it.
type orderedRoomSessions struct {
	roomSessions

	rows []meshcorehost.PersistedSession
}

func (s orderedRoomSessions) LoadSessions() ([]meshcorehost.PersistedSession, error) {
	return append([]meshcorehost.PersistedSession(nil), s.rows...), nil
}

func TestRoomRebuildKeepsAdminsWhenCapacityShrinks(t *testing.T) {
	previous := benchRoom(t, nil)
	admin, reader := newClient(t, previous), newClient(t, previous)
	rows := []meshcorehost.PersistedSession{
		{PubKey: reader.id.PubKey, Perms: mesh.PermReadWrite, LastActive: time.Now()},
		{PubKey: admin.id.PubKey, Perms: mesh.PermAdmin, LastActive: time.Now().Add(-time.Hour), LastTimestamp: 123},
	}
	cfg := baseConfig()
	cfg["max_members"] = 1
	spec := application.Spec{Name: "lobby", Config: cfg, Sessions: orderedRoomSessions{rows: rows}}
	if err := checkStored(spec); err != nil {
		t.Fatal(err)
	}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	room, ok := built.(*service)
	if !ok {
		t.Fatalf("room builder returned %T", built)
	}
	if got := room.table.Live(admin.id.PubKey); got == nil || !got.IsAdmin() || got.LastTimestamp != 123 {
		t.Fatalf("a fresher reader displaced the admin or its guard on rebuild: %+v", got)
	}
	if room.table.Live(reader.id.PubKey) != nil {
		t.Fatal("the reduced table seated the unprotected reader")
	}

	rows[0].Perms = mesh.PermAdmin
	for name, check := range map[string]func() error{
		"preflight": func() error { return checkStored(spec) },
		"startup":   func() error { _, err := build(spec); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil || !strings.Contains(err.Error(), "2") {
				t.Fatalf("a room smaller than its admin population was accepted: %v", err)
			}
		})
	}
}
