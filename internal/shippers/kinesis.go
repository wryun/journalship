package shippers

import (
	"crypto/md5"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"

	proto "github.com/golang/protobuf/proto"
	"github.com/wryun/journalship/internal/reader"
)

var (
	magicNumber = [4]byte{0xF3, 0x89, 0x9A, 0xC2}
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
	contents       []byte
	chunkSize      int
	readerChunkIDs []reader.ChunkID
}

func (k *KinesisOutputChunk) AddChunkID(chunkID *reader.ChunkID) {
	k.readerChunkIDs = append(k.readerChunkIDs, *chunkID)
}

func (k *KinesisOutputChunk) IsEmpty() bool {
	return len(k.contents) == 0
}

func (k *KinesisOutputChunk) Add(entry interface{}) (bool, error) {
	rawEntry, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	// TODO protobuf encode
	partitionKeyIndex := uint64(0)
	ag := AggregatedRecord{
		Records: []*Record{
			{
				PartitionKeyIndex: &partitionKeyIndex,
				Data:              rawEntry,
			},
		},
	}
	encoded, err := proto.Marshal(&ag)
	if err != nil {
		return false, err
	}

	if len(k.contents)+len(encoded) > k.chunkSize {
		return false, nil
	}

	k.contents = append(k.contents, encoded...)
	return true, nil
}

func (k *KinesisOutputChunk) makeContainer() []byte {
	hash := md5.Sum(k.contents)
	result := make([]byte, len(magicNumber)+len(k.contents)+len(hash))
	result = append(result, magicNumber[:]...)
	result = append(result, k.contents...)
	return append(result, hash[:]...)
}

type KinesisShipper struct {
	chunkSize int
}

func (k *KinesisShipper) NewOutputChunk() OutputChunk {
	// confusing magic. Because of the format of protobuf records,
	// it's valid (in this case) to concatenate the AggregatedRecords
	// to form one AggregatedRecord, since we only have a repeated field
	// in records.
	ag := AggregatedRecord{
		PartitionKeyTable: []string{strconv.FormatUint(rand.Uint64(), 36)},
	}
	encoded, err := proto.Marshal(&ag)
	if err != nil {
		log.Fatal(err)
	}
	return &KinesisOutputChunk{
		contents:  encoded,
		chunkSize: k.chunkSize,
	}
}

func (k *KinesisShipper) Run(outputChunksChannel chan OutputChunk, cursorSaver *reader.CursorSaver) {
	for {
		outputChunk := <-outputChunksChannel
		kinesisOutputChunk := outputChunk.(*KinesisOutputChunk)
		_ = kinesisOutputChunk.makeContainer()
		cursorSaver.ReportCompleted(kinesisOutputChunk.readerChunkIDs)
	}
}
