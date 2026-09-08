package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
)

func runtimeForRemoval(t *testing.T, m *manager, kind string) string {
	t.Helper()
	if kind == confdb.KindRelay {
		m.mu.Lock()
		m.startRelay(m.ctx, "meshcore-868")
		m.mu.Unlock()
		return "meshcore-868"
	}
	if _, err := m.Create(t.Context(), confdb.KindStation, "alice", map[string]string{
		"protocol": "meshcore", "listen": freeTCPAddr(t), "profile": "eu-868-narrow",
		"identity": "new", "node_name": "Alice", "tx_power_dbm": "14",
	}, "test"); err != nil {
		t.Fatal(err)
	}
	return "alice"
}

// A final writer stands for a command admitted before cancellation.
// Its commit is deliberately delayed until shutdown: deletion must join
// it, regardless of whether its store operation observes cancellation.
func runtimeWithFinalWrite(t *testing.T, m *manager, kind, name string) <-chan error {
	t.Helper()
	written := make(chan error, 1)
	run := func(ctx context.Context) error {
		<-ctx.Done()
		var err error
		if kind == confdb.KindStation {
			err = m.store.SaveStationState(context.WithoutCancel(ctx), name, []byte(`{"version":1}`))
		} else {
			err = m.store.SaveACL(context.WithoutCancel(ctx), name, confdb.ACLRow{
				PubKey: make([]byte, 32), Perms: 3, LastTimestamp: 100, LastActive: time.Now(),
			})
		}
		written <- err
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if kind == confdb.KindStation {
		h := m.stations[name]
		m.stopHost(stationHosting, name, &h.managedHost)
		m.runHost(m.ctx, stationHosting, name, &h.managedHost, run, nil)
	} else {
		m.stopRelay(name)
		ctx, cancel := context.WithCancel(m.ctx)
		h := &managedRelay{cancel: cancel, done: make(chan struct{})}
		m.running[name] = h
		m.wg.Go(func() {
			defer close(h.done)
			_ = run(ctx)
		})
	}
	return written
}

func TestRemovingRuntimeJoinsItsWritersBeforeDeletingState(t *testing.T) {
	for _, kind := range []string{confdb.KindRelay, confdb.KindStation} {
		t.Run(kind, func(t *testing.T) {
			m := lifecycleManager(t)
			name := runtimeForRemoval(t, m, kind)
			written := runtimeWithFinalWrite(t, m, kind, name)
			if _, err := m.Remove(t.Context(), kind, name, "test"); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("removal did not join the final write")
			}
			switch kind {
			case confdb.KindStation:
				if _, found, err := m.store.LoadStationState(t.Context(), name); err != nil || found {
					t.Fatalf("station state survived removal: found=%v, error=%v", found, err)
				}
				if len(m.StationInfos()) != 0 {
					t.Fatal("removed station remains visible")
				}
			case confdb.KindRelay:
				if rows, err := m.store.LoadACL(t.Context(), name); err != nil || len(rows) != 0 {
					t.Fatalf("relay access state survived removal: entries=%d, error=%v", len(rows), err)
				}
				if m.running[name] != nil {
					t.Fatal("removed relay remains running")
				}
			}
		})
	}
}

func TestFailedRuntimeRemovalRestoresTheOriginalConfiguration(t *testing.T) {
	for _, kind := range []string{confdb.KindRelay, confdb.KindStation} {
		t.Run(kind, func(t *testing.T) {
			m := lifecycleManager(t)
			name := runtimeForRemoval(t, m, kind)
			before, err := cloneFile(m.file)
			if err != nil {
				t.Fatal(err)
			}
			storedBefore, err := m.store.Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			previousRelay, previousStation := m.running[name], m.stations[name]
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := m.Remove(ctx, kind, name, "test"); !errors.Is(err, context.Canceled) {
				t.Fatalf("removal with a canceled transaction = %v", err)
			}
			stored, err := m.store.Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			after, err := cloneFile(m.file)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after.Relays, before.Relays) ||
				!reflect.DeepEqual(after.Stations, before.Stations) ||
				!reflect.DeepEqual(stored.Relays, storedBefore.Relays) ||
				!reflect.DeepEqual(stored.Stations, storedBefore.Stations) {
				t.Fatal("a refused removal changed the configured services")
			}
			var done <-chan struct{}
			if kind == confdb.KindStation {
				done = previousStation.done
				current := m.stations[name]
				if current == nil || current == previousStation || current.service == nil {
					t.Fatal("the stopped station was not rebuilt")
				}
				if current.service.Info().PublicKey != previousStation.service.Info().PublicKey {
					t.Fatal("the restored station changed its identity")
				}
			} else {
				done = previousRelay.done
				if current := m.running[name]; current == nil || current == previousRelay {
					t.Fatal("the stopped relay was not rebuilt")
				}
			}
			select {
			case <-done:
			default:
				t.Fatal("the previous service was not joined")
			}
		})
	}
}
