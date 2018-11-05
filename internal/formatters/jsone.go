package formatters

import (
	"encoding/json"

	jsone "github.com/taskcluster/json-e"
)

func NewJsoneFormatter(rawConfig json.RawMessage) (FormatEntry, error) {
	var config struct {
		Template interface{} `json:"template"`
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	return func(entry *Entry) error {
		var err error
		entry.Fields, err = jsone.Render(config.Template, map[string]interface{}{
			"fields": entry.Fields,
		})
		return err
	}, nil
}
