package formatters

import (
	"encoding/json"
	"errors"

	"github.com/wryun/journalship/internal"
)

func NewAddFormatter(rawConfig json.RawMessage) (FormatEntry, error) {
	var config struct {
		Fields map[string]interface{} `json:"fields"`
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	if config.Fields == nil {
		return nil, errors.New("must specify fields to add")
	}

	return func(entry *internal.Entry) error {
		for k, v := range config.Fields {
			entry.Fields[k] = v
		}
		return nil
	}, nil
}
