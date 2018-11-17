package formatters

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/wryun/journalship/internal"
)

func NewUnmarshalFormatter(rawConfig json.RawMessage) (FormatEntry, error) {
	var config struct {
		InputPath string `json:"inputPath"`
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	if config.InputPath == "" {
		return nil, errors.New("must specify field to unmarshal")
	}

	inputPath := strings.Split(config.InputPath, ".")

	return func(entry *internal.Entry) error {
		// TODO not sure about error handling here.
		// At the moment, we simply bail if any issues, but user
		// then misses what they've done wrong...
		// Need debugging log mode?
		var something interface{}
		something = entry.Fields
		var fieldMap map[string]interface{}
		var f string
		for _, f = range inputPath {
			var ok bool
			fieldMap, ok = something.(map[string]interface{})
			if !ok {
				return nil
			}
			if something, ok = fieldMap[f]; !ok {
				return nil
			}
		}
		input, ok := something.(string)
		if !ok {
			return nil
		}

		delete(fieldMap, f)
		err := json.Unmarshal([]byte(input), &entry.Fields)
		if err != nil {
			fieldMap[f] = something
			return nil
		}

		// TODO support outputPath?
		return nil
	}, nil
}
