package reader

// #include <systemd/sd-journal.h>
import "C"

import (
	"encoding/json"
	"log"
	"io/ioutil"

	"github.com/wryun/journalship/internal"
)

type Reader struct {
	journal *C.sd_journal
	entriesInChunk int
	fieldNames []string
	cursorFile string
	CursorSaver *CursorSaver
}

func NewReader(rawConfig json.RawMessage) (*Reader, error) {
	config := struct {
		CursorFile            string `json:"cursorFile"`
		EntriesInChunk int    `json:"entriesInChunk"`
		DataThreshold int `json:"dataThreshold"`
		FieldNames []string `json:"fieldNames"`
	}{
		CursorFile: "",
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

	usedCursor := false
	if config.CursorFile != "" {
		if contents, err := ioutil.ReadFile(config.CursorFile); err != nil {
			log.Printf("unable to load cursor file: %s", config.CursorFile)
		} else {
			err := journal.SeekCursor(string(contents))
			if err != nil {
				return nil, err
			}
			log.Printf("starting at cursor: %q", contents)
			usedCursor = true
		}
	}

	if !usedCursor {
		err = journal.SeekHead()
		if err != nil {
			return nil, err
		}
		log.Print("starting at beginning")
	}



	return &Reader{
		journal: journal,
		entriesInChunk: config.EntriesInChunk,
		fieldNames: config.FieldNames,
		cursorFile: config.CursorFile,
		CursorSaver: newCursorSaver(config.CursorFile),
	}, nil
}

func (r *Reader) Run(inputChunksChannel chan InputChunk) {
	// We send chunks rather than single entries through the channel so we can transfer
	// data more quickly. Premature optimisation something something...
	inputChunk := NewInputChunk(r.entriesInChunk)
	count := uint64(0)

	for {
		n, err := r.journal.Next()
		if err != nil {
			log.Fatal(err)
		}

		if inputChunk.isFull() || n == 0 && !inputChunk.isEmpty() {
			if r.cursorFile != "" {
				cursor, err := r.journal.GetCursor()
				if err != nil {
					log.Fatal("unable to use find cursor???")
				}
				inputChunk.id = ChunkID{cursor: cursor, order: count}
				count = count + 1
			}
			r.CursorSaver.ReportInFlight(inputChunk.ID())
			inputChunksChannel <- inputChunk
			inputChunk = NewInputChunk(r.entriesInChunk)
		}

		if n == 0 {
			r.journal.Wait(indefiniteWait)
			continue
		}

		entry, err := r.readEntry()
		if err != nil {
			log.Printf("dropped entry: %s", err)
			// TODO
			continue
		}

		// TODO reassembling of CONTAINER_PARTIAL ...

		inputChunk.addEntry(entry)
	}

}

func (r *Reader) readEntry() (*internal.Entry, error) {
	var fields map[string]interface{}
	var err error
	if r.fieldNames == nil {
		fields, err = r.journal.GetFields()
		if err != nil {
			return nil, err
		}
	} else {
		fields = make(map[string]interface{})
		for _, fieldName := range r.fieldNames {
			v, err := r.journal.GetField(fieldName)
			if err != nil {
				return nil, err
			}
			if v != nil {
				fields[fieldName] = *v
			}
		}
	}
	return &internal.Entry{Fields: fields}, nil
}