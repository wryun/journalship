package reader

// #include <systemd/sd-journal.h>
import "C"

import (
	"encoding/json"
	"log"
	"io/ioutil"
	"time"

	"github.com/wryun/journalship/internal"
)

type Reader struct {
	journal *C.sd_journal
	entriesInChunk int
	fieldNames []string
	cursorFile string
	joinContainerPartial int
	partialBuffer map[string]*internal.Entry
	timeField string
	CursorSaver *CursorSaver
}

func NewReader(rawConfig json.RawMessage) (*Reader, error) {
	config := struct {
		CursorFile            string `json:"cursorFile"`
		EntriesInChunk int    `json:"entriesInChunk"`
		DataThreshold int `json:"dataThreshold"`
		FieldNames []string `json:"fieldNames"`
		JoinContainerPartial int `json:"joinContainerPartial"`
		TimeField string `json:"timeField"`
	}{
		CursorFile: "",
		EntriesInChunk: 1000,
		DataThreshold: 0,
		FieldNames: nil,
		JoinContainerPartial: 0,
		TimeField: "TIME",
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
		joinContainerPartial: config.JoinContainerPartial,
		partialBuffer: make(map[string]*internal.Entry),
		timeField: config.TimeField,
		CursorSaver: newCursorSaver(config.CursorFile),
	}, nil
}

func (r *Reader) Run(inputChunksChannel chan InputChunk) {
	// We send chunks rather than single entries through the channel so we can transfer
	// data more quickly. Premature optimisation something something...
	inputChunk := NewInputChunk(r.entriesInChunk)
	count := uint64(1) // 0 is reserved for 'we're not using ids'

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
				inputChunk.id = ChunkID{cursor: cursor, id: count}
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

		if r.joinContainerPartial == 0 {
			inputChunk.addEntry(entry)
		} else {
			entries, err := r.joinEntry(entry)
			if err != nil {
				log.Printf("dropped entry: %s", err)
				// TODO
				continue
			}
			for _, entry := range entries {
				inputChunk.addEntry(entry)
			}
		}
	}
}

func (r *Reader) joinEntry(entry *internal.Entry) ([]*internal.Entry, error) {
	containerID, err := r.journal.GetField("CONTAINER_ID_FULL")
	if err != nil {
		return nil, err
	}
	if containerID == nil {
		return []*internal.Entry{entry}, nil
	}

	v, err := r.journal.GetField("CONTAINER_PARTIAL_MESSAGE")
	if err != nil {
		return nil, err
	}
	partialMessage := v != nil && *v == "true"

	if existingEntry, ok := r.partialBuffer[*containerID]; ok {
		proposedMessage := (existingEntry.Fields["MESSAGE"].(string) +
			entry.Fields["MESSAGE"].(string))

		if len(proposedMessage) > r.joinContainerPartial {
			existingEntry.Fields["MESSAGE"] = proposedMessage[:r.joinContainerPartial]
			entry.Fields["MESSAGE"] = proposedMessage[r.joinContainerPartial:]
			if partialMessage {
				r.partialBuffer[*containerID] = entry
				return []*internal.Entry{existingEntry}, nil
			}

			delete(r.partialBuffer, *containerID)
			return []*internal.Entry{existingEntry, entry}, nil
		}

		if partialMessage {
			existingEntry.Fields["MESSAGE"] = proposedMessage
			return []*internal.Entry{}, nil
		}

		entry.Fields["MESSAGE"] = proposedMessage
		delete(r.partialBuffer, *containerID)
		return []*internal.Entry{entry}, nil
	} else if partialMessage {
		r.partialBuffer[*containerID] = entry
		return []*internal.Entry{}, nil
	}

	return []*internal.Entry{entry}, nil
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

	if r.timeField != "" {
		entryTime, err := r.journal.GetRealtime()
		if err != nil {
			log.Fatal(err)
		}
		fields[r.timeField] = entryTime.Format(time.RFC3339Nano)
	}

	return &internal.Entry{Fields: fields}, nil
}