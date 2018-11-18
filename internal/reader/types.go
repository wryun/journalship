package reader

import (
	"github.com/wryun/journalship/internal"
)

type ChunkID struct {
	id     uint64
	cursor string
}

type InputChunk struct {
	entries        []*internal.Entry
	entriesInChunk int
	id             ChunkID
}

func NewInputChunk(entriesInChunk int) InputChunk {
	return InputChunk{
		entries:        make([]*internal.Entry, 0, entriesInChunk),
		entriesInChunk: entriesInChunk,
	}
}

func (ic *InputChunk) isFull() bool {
	return len(ic.entries) >= ic.entriesInChunk
}

func (ic *InputChunk) isEmpty() bool {
	return len(ic.entries) == 0
}

func (ic *InputChunk) addEntry(entry *internal.Entry) {
	ic.entries = append(ic.entries, entry)
}

func (ic *InputChunk) ID() *ChunkID {
	return &ic.id
}

func (ic *InputChunk) IntID() uint64 {
	return ic.id.id
}

func (ic *InputChunk) GetEntries() []*internal.Entry {
	return ic.entries
}
