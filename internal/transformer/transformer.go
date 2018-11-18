package transformer

import (
	"encoding/json"
	"log"
	"time"

	"github.com/wryun/journalship/internal/formatters"
	"github.com/wryun/journalship/internal/reader"
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

func addChunkID(outputChunk shippers.OutputChunk, id uint64) {
	if id != 0 {
		outputChunk.AddChunkID(id)
	}
}

func (t *Transformer) Run(inputChunksChannel chan reader.InputChunk, cursorSaver *reader.CursorSaver, outputChunksChannel chan shippers.OutputChunk) {
	outputChunk := t.newOutputChunk()
	lastShipTime := time.Now()

	shipChunk := func() {
		outputChunksChannel <- outputChunk
		outputChunk = t.newOutputChunk()
		lastShipTime = time.Now()
	}

	for {
		var inputChunk reader.InputChunk
		if outputChunk.IsEmpty() {
			inputChunk = <-inputChunksChannel
		} else {
			select {
			case inputChunk = <-inputChunksChannel:
			case <-makeTimeout(t.maxLogDelay - time.Now().Sub(lastShipTime)):
				shipChunk()
				continue
			}
		}

		for _, entry := range inputChunk.GetEntries() {
			for _, formatFn := range t.formatFns {
				if err := formatFn(entry); err != nil {
					// TODO
					log.Println(err)
				}
				if entry.Fields == nil {
					break
				}
			}

			if entry.Fields == nil {
				continue
			}

			added, err := outputChunk.Add(entry.Fields)
			if err != nil {
				// TODO
				log.Println(err)
				continue
			}
			if added {
				continue
			}

			if !outputChunk.IsEmpty() {
				addChunkID(outputChunk, inputChunk.IntID())
				// this is still in flight, so add another entry to
				// our reported tracking (so we don't accidentally complete
				// this early)
				cursorSaver.ReportInFlight(inputChunk.ID())
				shipChunk()
			}

			added, err = outputChunk.Add(entry.Fields)
			if err != nil {
				// TODO
				log.Println(err)
			} else if !added {
				// TODO
				log.Println("single log entry too large!")
			}
		}

		if outputChunk.IsEmpty() {
			// In whatever was left of this inputChunk, we didn't add
			// anything to our outputChunk, so we misreported that it
			// was in flight (i.e. must report it complete now so it
			// doesn't block everything up...)
			cursorSaver.ReportCompleted([]uint64{inputChunk.IntID()})
		} else {
			addChunkID(outputChunk, inputChunk.IntID())
		}
	}
}
