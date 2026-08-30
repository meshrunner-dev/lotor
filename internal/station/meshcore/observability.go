package meshcore

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func companionCommandName(command companion.Command) string {
	// Convert away from the wire enum: this is a deliberately partial list
	// for command shapes whose Go type alone does not carry the actual verb.
	switch uint8(command.Code()) {
	case uint8(companion.CommandSyncNextMessage):
		return "SyncNextMessage"
	case uint8(companion.CommandResetPath):
		return "ResetPath"
	case uint8(companion.CommandRemoveContact):
		return "RemoveContact"
	case uint8(companion.CommandShareContact):
		return "ShareContact"
	case uint8(companion.CommandGetContactByKey):
		return "GetContactByKey"
	case uint8(companion.CommandGetDeviceTime):
		return "GetDeviceTime"
	case uint8(companion.CommandGetBatteryAndStorage):
		return "GetBatteryAndStorage"
	case uint8(companion.CommandGetTuningParams):
		return "GetTuningParams"
	case uint8(companion.CommandGetAutoAddConfig):
		return "GetAutoAddConfig"
	case uint8(companion.CommandGetAllowedRepeatFreq):
		return "GetAllowedRepeatFreq"
	case uint8(companion.CommandGetDefaultFloodScope):
		return "GetDefaultFloodScope"
	case uint8(companion.CommandExportPrivateKey):
		return "ExportPrivateKey"
	case uint8(companion.CommandSignStart):
		return "SignStart"
	case uint8(companion.CommandSignFinish):
		return "SignFinish"
	case uint8(companion.CommandGetCustomVars):
		return "GetCustomVars"
	}
	typ := reflect.TypeOf(command)
	if typ != nil {
		return typ.Name()
	}
	return fmt.Sprintf("command-%d", command.Code())
}

func responseCode(payload []byte) uint8 {
	if len(payload) == 0 {
		return 0
	}
	return payload[0]
}

// logCompanionMutation runs only after persistence succeeded. It deliberately
// reports attribute names, not values: the same path carries private identity
// and channel material alongside harmless preferences.
func (s *service) logCompanionMutation(command companion.Command, responses []companion.Response,
	before, after persistedState,
) {
	if command.Code() == companion.CommandSyncNextMessage {
		if len(after.Mailbox) >= len(before.Mailbox) || len(before.Mailbox) == 0 {
			return
		}
		fields := []zap.Field{
			zap.String("message", mailboxKind(before.Mailbox[0])),
			zap.Int("remaining", len(after.Mailbox)),
		}
		if len(before.MailboxCorr) > 0 && !before.MailboxCorr[0].IsZero() {
			fields = append(fields, zap.String("corr", before.MailboxCorr[0].Short()))
		}
		logging.Trace(s.log, "station mailbox delivered", fields...)
		return
	}
	if !mutationSucceeded(command, responses) {
		return
	}
	attributes := stationStateChanges(before, after)
	if command.Code() == companion.CommandSetFloodScopeKey {
		attributes = append(attributes, "flood_scope")
	}
	if len(attributes) == 0 {
		switch command.(type) {
		case companion.Reboot, companion.FactoryReset:
		default:
			return
		}
	}
	s.log.Debug("station configuration changed", zap.String("source", "companion"),
		zap.String("command", companionCommandName(command)),
		zap.Uint8("code", uint8(command.Code())), zap.Strings("attributes", attributes))
}

func mutationSucceeded(command companion.Command, responses []companion.Response) bool {
	if _, reboot := command.(companion.Reboot); reboot {
		return len(responses) == 0
	}
	if len(responses) == 0 {
		return false
	}
	status, ok := responses[0].(companion.StatusResponse)
	return ok && status == companion.StatusResponse(companion.ResponseOK)
}

func stationStateChanges(before, after persistedState) []string {
	changes := make([]string, 0, 12)
	add := func(changed bool, name string) {
		if changed {
			changes = append(changes, name)
		}
	}
	add(before.Waveform.FrequencyHz != after.Waveform.FrequencyHz, "frequency_hz")
	add(before.Waveform.SpreadingFactor != after.Waveform.SpreadingFactor, "spreading_factor")
	add(before.Waveform.BandwidthHz != after.Waveform.BandwidthHz, "bandwidth_hz")
	add(before.Waveform.CodingRate != after.Waveform.CodingRate, "coding_rate")
	add(before.Waveform.Preamble != after.Waveform.Preamble, "preamble")
	add(before.Waveform.SyncWord != after.Waveform.SyncWord, "sync_word")
	add(before.Waveform.CRC != after.Waveform.CRC, "crc")
	add(before.TXPowerDBm != after.TXPowerDBm, "tx_power_dbm")
	add(before.NodeName != after.NodeName, "node_name")
	add(before.NodeLat != after.NodeLat, "node_lat")
	add(before.NodeLon != after.NodeLon, "node_lon")
	add(before.PIN != after.PIN, "pin")
	add(before.MultiACKs != after.MultiACKs, "multi_acks")
	add(before.AdvertLoc != after.AdvertLoc, "advert_loc_policy")
	add(before.TelemetryMode != after.TelemetryMode, "telemetry_mode")
	add(before.ManualContact != after.ManualContact, "manual_add_contacts")
	add(before.PathHashMode != after.PathHashMode, "path_hash_mode")
	add(before.RXDelayMilli != after.RXDelayMilli, "rx_delay_milli")
	add(before.AirFactorMilli != after.AirFactorMilli, "airtime_factor_milli")
	add(!bytes.Equal(before.PrivateKey, after.PrivateKey), "identity")
	add(before.ClockDelta != after.ClockDelta, "device_time")
	add(before.AutoFlags != after.AutoFlags, "auto_add_flags")
	add(before.AutoHops != after.AutoHops, "auto_add_max_hops")
	add(before.DefaultScope != after.DefaultScope || before.DefaultKey != after.DefaultKey,
		"default_flood_scope")
	add(!reflect.DeepEqual(before.Channels, after.Channels), "channels")
	add(!reflect.DeepEqual(before.Contacts, after.Contacts), "contacts")
	return changes
}

func mailboxKind(payload []byte) string {
	if len(payload) == 0 {
		return "invalid"
	}
	switch companion.ResponseCode(payload[0]) {
	case companion.ResponseContactMessage:
		return "contact-message"
	case companion.ResponseContactMessageV3:
		return "contact-message-v3"
	case companion.ResponseChannelMessage:
		return "channel-message"
	case companion.ResponseChannelMessageV3:
		return "channel-message-v3"
	case companion.ResponseChannelData:
		return "channel-data"
	default:
		return fmt.Sprintf("response-%d", payload[0])
	}
}

func correlationFromContext(ctx context.Context) correlation.ID {
	id, _ := correlation.FromContext(ctx)
	return id
}

func firstCorrelation(ids []correlation.ID) correlation.ID {
	if len(ids) == 0 {
		return correlation.ID{}
	}
	return ids[0]
}

func contactPrefix(publicKey [mesh.PubKeySize]byte) string {
	return hex.EncodeToString(publicKey[:6])
}
