package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/coreos/go-systemd/sdjournal"
	jsone "github.com/taskcluster/json-e"
)

const (
	JournalEntryChunkSize = 100
	RawDataChunkSize      = 200000
	NumShippers           = 10
	NumTransformers       = 5
	MaxLogDelay           = 3 * time.Second // how long to wait when batching
	CursorFileLocation    = "shipit.cursor"
)

func main() {
	journal, err := loadJournal()
	if err != nil {
		log.Fatal(err)
	}

	// We split formatters (CPU intensive) and shippers (likely network intensive,
	// delays) so they can be separately tuned.
	journalEntriesChannel := make(chan []*sdjournal.JournalEntry)
	outputChunksChannel := make(chan OutputChunk)

	shipper := KinesisShipper{
		outputChunksChannel: outputChunksChannel,
	}

	template := map[string]interface{}{}

	transformer := Transformer{
		journalEntriesChannel: journalEntriesChannel,
		outputChunksChannel:   outputChunksChannel,
		newOutputChunk:        shipper.NewOutputChunk,
		formatEntry: func(entry *sdjournal.JournalEntry) (interface{}, error) {
			return jsone.Render(template, map[string]interface{}{
				"fields": entry.Fields,
			})
		},
	}

	// TODO formatters should really be a dynamically resizing pool (?)
	for i := 0; i < NumTransformers; i++ {
		go transformer.Run()
	}
	for i := 0; i < NumShippers; i++ {
		go shipper.Run()
	}

	// It only makes sense to have one reader due to how journald works.
	reader(journal, journalEntriesChannel)
}

func loadJournal() (*sdjournal.Journal, error) {
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

	return journal, err
}

func reader(journal *sdjournal.Journal, journalEntriesChannel chan []*sdjournal.JournalEntry) {
	// We send chunks rather than single entries through the channel so we can transfer
	// data more quickly. Premature optimisation something something...
	// (might do better removing the unnecessary lock from the go journald library)
	journalEntriesChunk := make([]*sdjournal.JournalEntry, 0, JournalEntryChunkSize)

	for {
		n, err := journal.Next()
		if err != nil {
			log.Println(err)
			// TODO ...
			continue
		}

		if (n == 0 && len(journalEntriesChunk) > 0) || len(journalEntriesChunk) >= JournalEntryChunkSize {
			journalEntriesChannel <- journalEntriesChunk
			journalEntriesChunk = make([]*sdjournal.JournalEntry, 0, JournalEntryChunkSize)
		}

		if n == 0 {
			journal.Wait(sdjournal.IndefiniteWait)
			continue
		}

		entry, err := journal.GetEntry()
		if err != nil {
			log.Println(err)
			// TODO
			continue
		}

		// TODO reassembling of CONTAINER_PARTIAL ...

		journalEntriesChunk = append(journalEntriesChunk, entry)
	}

}

type Transformer struct {
	journalEntriesChannel chan []*sdjournal.JournalEntry
	outputChunksChannel   chan OutputChunk
	newOutputChunk        func() OutputChunk
	formatEntry           func(entry *sdjournal.JournalEntry) (interface{}, error)
}

func makeTimeout(duration time.Duration) chan bool {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(duration)
		timeout <- true
	}()
	return timeout
}

func (t *Transformer) Run() {
	chunk := t.newOutputChunk()
	lastShipTime := time.Now()

	shipChunk := func() {
		t.outputChunksChannel <- chunk
		chunk = t.newOutputChunk()
		lastShipTime = time.Now()
	}

	for {
		var entries []*sdjournal.JournalEntry
		if !chunk.IsEmpty() {
			select {
			case <-makeTimeout(MaxLogDelay - time.Now().Sub(lastShipTime)):
				shipChunk()
				continue
			case entries = <-t.journalEntriesChannel:
			}
		} else {
			entries = <-t.journalEntriesChannel
		}

		for _, entry := range entries {
			entryFields, err := t.formatEntry(entry)
			if err != nil {
				// TODO
				log.Println(err)
				continue
			}
			added, err := chunk.Add(entryFields)
			if err != nil {
				// TODO
				log.Println(err)
				continue
			}
			if added {
				continue
			}

			shipChunk()
			added, err = chunk.Add(entryFields)
			if err != nil {
				// TODO
				log.Println(err)
			} else if !added {
				// TODO
				log.Println("single log entry too large!")
			}
		}
	}
}

type OutputChunk interface {
	IsEmpty() bool
	Add(interface{}) (bool, error)
}

type KinesisOutputChunk struct {
	contents []byte
}

func (k *KinesisOutputChunk) IsEmpty() bool {
	return len(k.contents) > 0
}

func (k *KinesisOutputChunk) Add(entry interface{}) (bool, error) {
	rawEntry, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	// TODO protobuf encode

	if len(k.contents)+len(rawEntry) > RawDataChunkSize {
		return false, nil
	}

	k.contents = append(k.contents, rawEntry...)
	return true, nil
}

type Shipper interface {
	NewOutputChunk() OutputChunk
	Run()
}

type KinesisShipper struct {
	outputChunksChannel chan OutputChunk
}

func (k *KinesisShipper) NewOutputChunk() OutputChunk {
	return &KinesisOutputChunk{
		contents: make([]byte, 0, RawDataChunkSize),
	}
}

func (k *KinesisShipper) Run() {
	for {
		_ = <-k.outputChunksChannel
		log.Println("Got some chunks!")
	}
}
