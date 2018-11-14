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
}

type Shipper interface {
	NewOutputChunk() OutputChunk
	Run(chan OutputChunk)
}
