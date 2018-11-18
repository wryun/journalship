package writer

import (
	"encoding/json"
	"log"

	"github.com/wryun/journalship/internal/reader"
	"github.com/wryun/journalship/internal/shippers"
)

type Writer struct {
}

func NewWriter(rawConfig json.RawMessage) (*Writer, error) {
	config := struct {
	}{}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	return &Writer{}, nil
}

func (w *Writer) Run(shipper shippers.ShipperInstance, outputChunksChannel chan shippers.OutputChunk, cursorSaver *reader.CursorSaver) {
	for {
		outputChunk := <-outputChunksChannel
		if err := shipper.Ship(outputChunk); err != nil {
			// if err retriable, track failure for other goroutines? (don't backoff only one chunk)
			// fail permanently if err not retriable? (config error? hmm)
			log.Fatal(err)
		}
		cursorSaver.ReportCompleted(outputChunk.GetChunkIDs())
	}
}
