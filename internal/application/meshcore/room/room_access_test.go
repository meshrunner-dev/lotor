package room

import (
	"testing"
	"time"

	mesh "meshrunner.dev/pkg/meshcore"
)

func TestTheAccessListFitsTheReplyRoute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		admins int
		flood  bool
		hops   int
		width  int
		rows   int
	}{
		{"direct", 25, false, 0, 1, 24},
		{"flood-zero-hop", 23, true, 0, 1, 22},
		{"flood-18-hops", 20, true, 18, 1, 19},
		{"flood-wide-hashes", 20, true, 18, 3, 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := benchRoomTuned(t, nil, func(cfg map[string]any) { cfg["max_members"] = 32 })
			var asker client
			for range tc.admins {
				asker = newClient(t, svc)
				login(t, svc, asker, "sesame", 0)
			}
			req, err := mesh.BuildRequest(asker.id, svc.id.PubKey[:], asker.secret,
				uint32(time.Now().Unix()), mesh.FrameAccessListRequest())
			if err != nil {
				t.Fatal(err)
			}
			route, budget := mesh.RouteDirect, mesh.ResponseBodyBudget()
			if tc.flood {
				route, budget = mesh.RouteFlood, mesh.PathReturnBodyBudget(tc.hops*tc.width)
			}
			req.Header = mesh.MakeHeader(route, mesh.PayloadTypeReq, mesh.PayloadVer1)
			req.SetPathHashSizeAndCount(tc.width, tc.hops)
			req.Path = make([]byte, tc.hops*tc.width)
			for i := range req.Path {
				req.Path[i] = byte(i + 1)
			}
			hear(t, svc, req)
			reply := emissionPacket(queued(t, svc))
			if tc.flood && reply.PayloadType() != mesh.PayloadTypePath {
				t.Fatal("flooded request was not answered inside a PATH return")
			}
			_, body, err := mesh.UnframeAdmin(openReply(t, asker, reply))
			if err != nil {
				t.Fatal(err)
			}
			rows := mesh.ParseAccessList(body)
			if len(body) > budget || len(rows) != tc.rows {
				t.Fatalf("access list = %d bytes, %d rows; budget %d, want %d rows", len(body), len(rows), budget, tc.rows)
			}
			for _, row := range rows {
				if mesh.Role(row.Permissions) != mesh.PermAdmin {
					t.Fatal("access list included a non-admin")
				}
			}
		})
	}
}

func TestPrivateRoomScopesRequireAKeyStore(t *testing.T) {
	for _, name := range []string{"$private", "$"} {
		cfg := baseConfig()
		cfg["default_scope"] = name
		if err := check(cfg); err == nil {
			t.Fatalf("private scope %q was accepted without a keystore", name)
		}
	}
	for _, name := range []string{"", "lyon", "#lyon", "#$public"} {
		cfg := baseConfig()
		cfg["default_scope"] = name
		if err := check(cfg); err != nil {
			t.Fatalf("public scope %q rejected: %v", name, err)
		}
	}
}
