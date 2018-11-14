package transformer

import (
	"encoding/json"
	"log"
	"time"

	"github.com/wryun/journalship/internal"
	"github.com/wryun/journalship/internal/formatters"
	"github.com/wryun/journalship/internal/shippers"
)

type Transformer struct {
	newOutputChunk func() shippers.OutputChunk
	formatFns      []formatters.FormatEntry
	maxLogDelay    time.Duration
}

func NewTransformer(rawConfig json.RawMessage, formatFns []formatters.FormatEntry, newOutputChunk func() shippers.OutputChunk) (*Transformer, error) {
	config := struct {
		MaxLogDelay int `json:"maxLogDelay"`
	}{
		MaxLogDelay: 3,
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	return &Transformer{
		newOutputChunk: newOutputChunk,
		formatFns:      formatFns,
		maxLogDelay:    time.Duration(config.MaxLogDelay) * time.Second,
	}, nil
}

func makeTimeout(duration time.Duration) chan bool {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(duration)
		timeout <- true
	}()
	return timeout
}

func (t *Transformer) Run(journalEntriesChannel chan []*internal.Entry, outputChunksChannel chan shippers.OutputChunk) {
	chunk := t.newOutputChunk()
	lastShipTime := time.Now()

	shipChunk := func() {
		outputChunksChannel <- chunk
		chunk = t.newOutputChunk()
		lastShipTime = time.Now()
	}

	for {
		var entries []*internal.Entry
		if !chunk.IsEmpty() {
			select {
			case <-makeTimeout(t.maxLogDelay - time.Now().Sub(lastShipTime)):
				shipChunk()
				continue
			case entries = <-journalEntriesChannel:
			}
		} else {
			entries = <-journalEntriesChannel
		}

		for _, entry := range entries {
			for _, formatFn := range t.formatFns {
				if err := formatFn(entry); err != nil {
					// TODO
					log.Println(err)
				}
			}

			added, err := chunk.Add(entry.Fields)
			if err != nil {
				// TODO
				log.Println(err)
				continue
			}
			if added {
				continue
			}

			shipChunk()
			added, err = chunk.Add(entry.Fields)
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
