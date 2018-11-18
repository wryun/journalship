package shippers

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
)

func NewFileShipper(rawConfig json.RawMessage) (Shipper, error) {
	config := struct {
		ChunkSize   int    `json:"chunkSize"`
		FileName    string `json:"fileName"`
		PrettyPrint int    `json:"prettyPrint"`
	}{
		ChunkSize:   200000,
		PrettyPrint: 0,
		FileName:    "", // default to stdout
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}

	out := os.Stdout
	if config.FileName != "" {
		var err error
		if out, err = os.Create(config.FileName); err != nil {
			return nil, err
		}
	}
	return &FileShipper{
		chunkSize:   config.ChunkSize,
		out:         out,
		prettyPrint: config.PrettyPrint,
	}, nil
}

type FileOutputChunk struct {
	contents       []byte
	chunkSize      int
	prettyPrint    int
	readerChunkIDs []uint64
}

func (fc *FileOutputChunk) AddChunkID(chunkID uint64) {
	fc.readerChunkIDs = append(fc.readerChunkIDs, chunkID)
}

func (fc *FileOutputChunk) GetChunkIDs() []uint64 {
	return fc.readerChunkIDs
}

func (fc *FileOutputChunk) IsEmpty() bool {
	return len(fc.contents) == 0
}

func (fc *FileOutputChunk) Add(entry interface{}) (bool, error) {
	var rawEntry []byte
	var err error
	if fc.prettyPrint != 0 {
		rawEntry, err = json.MarshalIndent(entry, "", strings.Repeat(" ", fc.prettyPrint))
	} else {
		rawEntry, err = json.Marshal(entry)
	}
	if err != nil {
		return false, err
	}

	if len(fc.contents)+len(rawEntry)+1 > fc.chunkSize {
		return false, nil
	}

	fc.contents = append(fc.contents, rawEntry...)
	fc.contents = append(fc.contents, []byte("\n")...)
	return true, nil
}

type FileShipper struct {
	chunkSize   int
	prettyPrint int
	out         io.WriteCloser
	mutex       sync.Mutex
}

func (fs *FileShipper) NewOutputChunk() OutputChunk {
	return &FileOutputChunk{
		contents:    make([]byte, 0, fs.chunkSize),
		chunkSize:   fs.chunkSize,
		prettyPrint: fs.prettyPrint,
	}
}

func (fs *FileShipper) Instance() ShipperInstance {
	return fs
}

func (fs *FileShipper) Ship(outputChunk OutputChunk) error {
	// TODO fs.out.Close() (need more features...)
	fileOutputChunk := outputChunk.(*FileOutputChunk)
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	_, err := fs.out.Write(fileOutputChunk.contents)
	return err
}
