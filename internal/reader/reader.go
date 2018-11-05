package reader

import (
	"encoding/json"
	"log"

	"github.com/coreos/go-systemd/sdjournal"
)

type Reader struct {
	journal               *sdjournal.Journal
	journalEntriesInChunk int
}

func NewReader(rawConfig json.RawMessage) (*Reader, error) {
	config := struct {
		CursorFile            string `json:"cursorFile"`
		JournalEntriesInChunk int    `json:"journalEntriesInChunk"`
	}{
		JournalEntriesInChunk: 100,
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	journal, err := sdjournal.NewJournal()
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
		journalEntriesInChunk: config.JournalEntriesInChunk,
	}, nil
}

func (r *Reader) Run(journalEntriesChannel chan []*sdjournal.JournalEntry) {
	// We send chunks rather than single entries through the channel so we can transfer
	// data more quickly. Premature optimisation something something...
	// (might do better removing the unnecessary lock from the go journald library)
	journalEntriesChunk := make([]*sdjournal.JournalEntry, 0, r.journalEntriesInChunk)

	for {
		n, err := r.journal.Next()
		if err != nil {
			log.Println(err)
			// TODO ...
			continue
		}

		if (n == 0 && len(journalEntriesChunk) > 0) || len(journalEntriesChunk) >= r.journalEntriesInChunk {
			journalEntriesChannel <- journalEntriesChunk
			journalEntriesChunk = make([]*sdjournal.JournalEntry, 0, r.journalEntriesInChunk)
		}

		if n == 0 {
			r.journal.Wait(sdjournal.IndefiniteWait)
			continue
		}

		entry, err := r.journal.GetEntry()
		if err != nil {
			log.Println(err)
			// TODO
			continue
		}

		// TODO reassembling of CONTAINER_PARTIAL ...

		journalEntriesChunk = append(journalEntriesChunk, entry)
	}

}
