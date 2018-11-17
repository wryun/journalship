package reader

import (
	"io/ioutil"
	"log"
	"sync"
)

type CursorSaver struct {
	cursorFile string
	mutex      sync.Mutex
	chunkIDs   []ChunkID
}

func newCursorSaver(cursorFile string) *CursorSaver {
	return &CursorSaver{
		cursorFile: cursorFile,
		chunkIDs:   make([]ChunkID, 0, 50),
		mutex:      sync.Mutex{},
	}
}

func (cs *CursorSaver) ReportCompleted(completedChunkIDs []ChunkID) {
	if cs.cursorFile == "" || len(completedChunkIDs) == 0 {
		return
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	for _, completedChunkID := range completedChunkIDs {
		foundChunk := false
		for i, chunkID := range cs.chunkIDs {
			if completedChunkID.order == chunkID.order {
				copy(cs.chunkIDs[i:], cs.chunkIDs[i+1:])
				cs.chunkIDs = cs.chunkIDs[:len(cs.chunkIDs)-1]
				if i == 0 {
					if err := ioutil.WriteFile(cs.cursorFile, []byte(chunkID.cursor), 0644); err != nil {
						log.Printf("unable to save cursor: %s", err)
					}
				}
				foundChunk = true
				break
			}
		}

		if !foundChunk {
			log.Fatalln("completed chunk not in flight - internal error", completedChunkID)
		}
	}
}

func (cs *CursorSaver) ReportInFlight(chunkID *ChunkID) {
	if cs.cursorFile == "" {
		return
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	var i int
	for i = len(cs.chunkIDs) - 1; i >= 0; i-- {
		if chunkID.order <= cs.chunkIDs[i].order {
			break
		}
	}

	insertAt := i + 1
	if len(cs.chunkIDs) == insertAt {
		cs.chunkIDs = append(cs.chunkIDs, *chunkID)
	} else {
		cs.chunkIDs = append(cs.chunkIDs, ChunkID{})
		copy(cs.chunkIDs[insertAt+1:], cs.chunkIDs[insertAt:])
		cs.chunkIDs[insertAt] = *chunkID
	}
}
