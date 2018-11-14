package formatters

import (
	"encoding/json"

	"github.com/wryun/journalship/internal"
)

type FormatEntry func(*internal.Entry) error
type FormatConstructor func(json.RawMessage) (FormatEntry, error)

var Formatters = map[string]FormatConstructor{
	"jsone": NewJsoneFormatter,
}
