package shippers

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/aws/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/golang/protobuf/proto"
)

var (
	magicNumber = [4]byte{0xF3, 0x89, 0x9A, 0xC2}
)

func NewKinesisShipper(rawConfig json.RawMessage) (Shipper, error) {
	config := struct {
		ChunkSize     int    `json:"chunkSize"`
		Region        string `json:"region"`
		StreamName    string `json:"streamName"`
		AssumeRoleArn string `json:"assumeRoleArn"`
	}{
		ChunkSize: 200000,
	}
	if err := json.Unmarshal(rawConfig, &config); err != nil {
		return nil, err
	}
	if config.Region == "" {
		return nil, errors.New("must specify region for kinesis")
	}
	if config.StreamName == "" {
		return nil, errors.New("must specify streamName for kinesis")
	}
	awsConfig, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, err
	}
	awsConfig.Region = config.Region
	if config.AssumeRoleArn != "" {
		awsConfig.Credentials = stscreds.NewAssumeRoleProvider(
			sts.New(awsConfig), config.AssumeRoleArn)
	}

	return &KinesisShipper{
		chunkSize:  config.ChunkSize,
		service:    kinesis.New(awsConfig),
		streamName: config.StreamName,
	}, nil
}

type KinesisOutputChunk struct {
	contents       []byte
	chunkSize      int
	readerChunkIDs []uint64
}

func (k *KinesisOutputChunk) AddChunkID(chunkID uint64) {
	k.readerChunkIDs = append(k.readerChunkIDs, chunkID)
}

func (k *KinesisOutputChunk) GetChunkIDs() []uint64 {
	return k.readerChunkIDs
}

func (k *KinesisOutputChunk) IsEmpty() bool {
	return len(k.contents) == 0
}

func (k *KinesisOutputChunk) Add(entry interface{}) (bool, error) {
	rawEntry, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
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
	chunkSize  int
	streamName string
	service    *kinesis.Kinesis
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

func (k *KinesisShipper) Instance() ShipperInstance {
	return k
}

func (k *KinesisShipper) Ship(outputChunk OutputChunk) error {
	kinesisOutputChunk := outputChunk.(*KinesisOutputChunk)
	partitionKey := strconv.FormatUint(rand.Uint64(), 36)
	req := k.service.PutRecordRequest(&kinesis.PutRecordInput{
		StreamName:   &k.streamName,
		Data:         kinesisOutputChunk.makeContainer(),
		PartitionKey: &partitionKey,
	})
	_, err := req.Send()
	return err
}
