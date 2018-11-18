package shippers

import (
	"encoding/json"
)

type ShipperConstructor func(json.RawMessage) (Shipper, error)

var Shippers = map[string]ShipperConstructor{
	"kinesis": NewKinesisShipper,
	"file":    NewFileShipper,
}

type OutputChunk interface {
	IsEmpty() bool
	Add(interface{}) (bool, error)
	AddChunkID(uint64)
	GetChunkIDs() []uint64
}

// Shipper allows shippers to control how their chunks are generated, in
// order to do as much processing as possible as the chunks come in
// (rather than have a common format and then spike the CPU when we go
// to ship a chunk).
type Shipper interface {
	NewOutputChunk() OutputChunk
	Instance() ShipperInstance
}

// ShipperInstances allow us (if we want) to have a separate config
// for each goroutine.
type ShipperInstance interface {
	Ship(OutputChunk) error
}
