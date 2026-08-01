package worker

// EnabledDevices is the operator-controlled device whitelist for a worker
// in explicit three-state form (mirrors HubSetEnabledDevices on the wire):
//
//   - All == true            → every advertised device is enabled; IDs is
//     ignored. The bootstrap default when MASS has no persisted intent.
//   - All == false, IDs set  → exactly those devices are enabled.
//   - All == false, IDs nil  → no devices are enabled; the worker must
//     reject new model loads.
//
// Emptiness never encodes intent: "none enabled" and "all enabled" are
// distinct values, so disabling every device round-trips faithfully.
type EnabledDevices struct {
	All bool
	IDs []string
}

// ComputeEnabledDevices maps persisted per-device toggle rows onto the
// explicit whitelist. state holds the persisted rows as deviceID → enabled;
// an empty state means the operator never toggled anything → All. With any
// rows present the result is the exact enabled subset of advertised, where
// a device without a row defaults to enabled (toggling one device off must
// not implicitly disable its siblings).
func ComputeEnabledDevices(advertised []string, state map[string]bool) EnabledDevices {
	if len(state) == 0 {
		return EnabledDevices{All: true}
	}
	ids := make([]string, 0, len(advertised))
	for _, id := range advertised {
		if enabled, ok := state[id]; !ok || enabled {
			ids = append(ids, id)
		}
	}
	return EnabledDevices{IDs: ids}
}
