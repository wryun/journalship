package formatters

import (
	"encoding/json"
	"strings"

	"github.com/wryun/journalship/internal"
)

func NewLowercaseFormatter(rawConfig json.RawMessage) (FormatEntry, error) {
	var config struct{}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	return func(entry *internal.Entry) error {
		newFields := make(map[string]interface{}, len(entry.Fields))
		for k, v := range entry.Fields {
			newFields[strings.ToLower(k)] = v
		}
		entry.Fields = newFields
		return nil
	}, nil
}
