package shippers

import (
	"encoding/json"
	"log"
)

func NewKinesisShipper(rawConfig json.RawMessage) (Shipper, error) {
	config := struct {
		ChunkSize int `json:"chunkSize"`
	}{
		ChunkSize: 200000,
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}
	return &KinesisShipper{
		chunkSize: config.ChunkSize,
	}, nil
}

type KinesisOutputChunk struct {
	contents  []byte
	chunkSize int
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

	if len(k.contents)+len(rawEntry) > k.chunkSize {
		return false, nil
	}

	k.contents = append(k.contents, rawEntry...)
	return true, nil
}

type KinesisShipper struct {
	chunkSize int
}

func (k *KinesisShipper) NewOutputChunk() OutputChunk {
	return &KinesisOutputChunk{
		contents:  make([]byte, 0, k.chunkSize),
		chunkSize: k.chunkSize,
	}
}

func (k *KinesisShipper) Run(outputChunksChannel chan OutputChunk) {
	for {
		_ = <-outputChunksChannel
		log.Println("Got some chunks!")
	}
}
