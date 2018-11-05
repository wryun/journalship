package formatters

import (
	"encoding/json"

	"github.com/coreos/go-systemd/sdjournal"
)

type FormatEntry func(*Entry) error
type FormatConstructor func(json.RawMessage) (FormatEntry, error)

// Entry echoes the output of sdjournal.Entry with only the relevant
// fields for log shipping. Fields is changed to an interface{}
// to allow formatters more flexibility.
type Entry struct {
	Fields             interface{}
	RealtimeTimestamp  uint64
	MonotonicTimestamp uint64
}

func NewEntry(entry *sdjournal.JournalEntry) Entry {
	return Entry{
		Fields:             entry.Fields,
		RealtimeTimestamp:  entry.RealtimeTimestamp,
		MonotonicTimestamp: entry.MonotonicTimestamp,
	}
}

var Formatters = map[string]FormatConstructor{
	"jsone": NewJsoneFormatter,
}
