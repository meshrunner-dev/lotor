//go:build lean

package relay

// NoiseHistoryDefault: the lean build spends no disk writes it was not
// explicitly asked for — the floor stays a RAM value unless the
// configuration turns archiving on.
const NoiseHistoryDefault = false
