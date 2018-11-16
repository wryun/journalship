package reader

// #include <systemd/sd-journal.h>
import "C"

import (
	"encoding/json"
	"log"

	"github.com/wryun/journalship/internal"
)

type Reader struct {
	journal               *C.sd_journal
	entriesInChunk int
	fieldNames []string
}

func NewReader(rawConfig json.RawMessage) (*Reader, error) {
	config := struct {
		CursorFile            string `json:"cursorFile"`
		EntriesInChunk int    `json:"entriesInChunk"`
		DataThreshold int `json:"dataThreshold"`
		FieldNames []string `json:"fieldNames"`
	}{
		CursorFile: "journalship.cursor",
		EntriesInChunk: 100,
		DataThreshold: 0,
		FieldNames: nil,
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	journal, err := NewJournal(config.DataThreshold)
	if err != nil {
		return nil, err
	}

	// TODO: SeekHead OR CursorFile...
	// (but... how do we save it nicely?)
	err = journal.SeekHead()
	if err != nil {
		return nil, err
	}
	return &Reader{
		journal:               journal,
		entriesInChunk: config.EntriesInChunk,
		fieldNames: config.FieldNames,
	}, nil
}

func (r *Reader) Run(entriesChannel chan []*internal.Entry) {
	// We send chunks rather than single entries through the channel so we can transfer
	// data more quickly. Premature optimisation something something...
	// (might do better removing the unnecessary lock from the go journald library)
	entriesChunk := make([]*internal.Entry, 0, r.entriesInChunk)

	for {
		n, err := r.journal.Next()
		if err != nil {
			log.Println(err)
			// TODO ...
			continue
		}

		if (n == 0 && len(entriesChunk) > 0) || len(entriesChunk) >= r.entriesInChunk {
			entriesChannel <- entriesChunk
			entriesChunk = make([]*internal.Entry, 0, r.entriesInChunk)
		}

		if n == 0 {
			r.journal.Wait(IndefiniteWait)
			continue
		}

		var fields map[string]interface{}
		if r.fieldNames == nil {
			fields, err = r.journal.GetFields()
			if err != nil {
				log.Println(err)
				// TODO
				continue
			}
		} else {
			fields = make(map[string]interface{})
			for _, fieldName := range r.fieldNames {
				v, err := r.journal.GetField(fieldName)
				if err != nil {
					log.Println(err)
					// TODO
					continue
				}
				if v != nil {
					fields[fieldName] = *v
				}
			}
		}

		// TODO reassembling of CONTAINER_PARTIAL ...

		entriesChunk = append(entriesChunk, &internal.Entry{Fields: fields})
	}

}
