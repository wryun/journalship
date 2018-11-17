package formatters

import (
	"encoding/json"
	"errors"

	jsone "github.com/taskcluster/json-e"
	"github.com/wryun/journalship/internal"
)

func NewJsoneFormatter(rawConfig json.RawMessage) (FormatEntry, error) {
	var config struct {
		Template interface{} `json:"template"`
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	return func(entry *internal.Entry) error {
		fields, err := jsone.Render(config.Template, map[string]interface{}{
			"fields": entry.Fields,
		})
		if err != nil {
			return err
		}
		var ok bool
		if entry.Fields, ok = fields.(map[string]interface{}); !ok {
			return errors.New("JSON-e transform returned non-dictionary")
		}
		return nil
	}, nil
}
