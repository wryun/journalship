package shippers

import (
	"encoding/json"
	"io"
	"os"
	"strings"
)

func NewFileShipper(rawConfig json.RawMessage) (Shipper, error) {
	config := struct {
		ChunkSize   int    `json:"chunkSize"`
		FileName    string `json:"fileName"`
		PrettyPrint int    `json:"prettyPrint"`
	}{
		ChunkSize:   4000,
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
	contents    []byte
	chunkSize   int
	prettyPrint int
}

func (fc *FileOutputChunk) IsEmpty() bool {
	return len(fc.contents) > 0
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

	if len(fc.contents)+len(rawEntry) > fc.chunkSize {
		return false, nil
	}

	fc.contents = append(fc.contents, rawEntry...)
	return true, nil
}

type FileShipper struct {
	chunkSize   int
	prettyPrint int
	out         io.WriteCloser
}

func (fs *FileShipper) NewOutputChunk() OutputChunk {
	return &FileOutputChunk{
		contents:    make([]byte, 0, fs.chunkSize),
		chunkSize:   fs.chunkSize,
		prettyPrint: fs.prettyPrint,
	}
}

func (fs *FileShipper) Run(outputChunksChannel chan OutputChunk) {
	defer fs.out.Close()

	for {
		outputChunk := <-outputChunksChannel
		if outputChunk == nil {
			break
		}
		fileOutputChunk := outputChunk.(*FileOutputChunk)
		fs.out.Write(fileOutputChunk.contents)
		fs.out.Write([]byte("\n"))
	}
}
