package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/coreos/go-systemd/sdjournal"
	"github.com/ghodss/yaml"

	"github.com/wryun/journalship/internal/formatters"
	"github.com/wryun/journalship/internal/reader"
	"github.com/wryun/journalship/internal/shippers"
	"github.com/wryun/journalship/internal/transformer"
)

type Config struct {
	NumTransformers int `json:"numTransformers"`
	NumShippers     int `json:"numShippers"`

	Transformer json.RawMessage `json:"transformer"`
	Reader      json.RawMessage `json:"reader"`

	Shipper    json.RawMessage   `json:"shipper"`
	Formatters []json.RawMessage `json:"formatters"`
}

type Plugin struct {
	Type string `json:"type"`
}

func main() {
	config := loadConfig()
	reader := configureReader(config.Reader)
	shipper := configureShipper(config.Shipper)
	transformer := configureTransformer(config.Transformer, config.Formatters, shipper.NewOutputChunk)

	journalEntriesChannel := make(chan []*sdjournal.JournalEntry)
	outputChunksChannel := make(chan shippers.OutputChunk)

	// TODO formatters should really be a dynamically resizing pool (?)
	for i := 0; i < config.NumTransformers; i++ {
		go transformer.Run(journalEntriesChannel, outputChunksChannel)
	}
	for i := 0; i < config.NumShippers; i++ {
		go shipper.Run(outputChunksChannel)
	}

	// It only makes sense to have one reader due to how journald works.
	reader.Run(journalEntriesChannel)
}

func loadConfig() Config {
	configFileName := flag.String("c", "", "file to use for config")
	flag.Parse()

	configFile, err := ioutil.ReadFile(*configFileName)
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		log.Fatal(err)
	}
	return config
}

func configureShipper(shipperConfig json.RawMessage) shippers.Shipper {
	var shipperPlugin Plugin
	if err := json.Unmarshal(shipperConfig, &shipperPlugin); err != nil {
		log.Fatal(err)
	}
	newShipper, ok := shippers.Shippers[shipperPlugin.Type]
	if !ok {
		log.Fatalf("No such shipper %q", shipperPlugin.Type)
	}
	shipper, err := newShipper(shipperConfig)
	if err != nil {
		log.Fatal(err)
	}
	return shipper
}

func configureTransformer(transformerConfig json.RawMessage, formattersConfig []json.RawMessage, newOutputChunk func() shippers.OutputChunk) *transformer.Transformer {
	formatFns := make([]formatters.FormatEntry, 0, len(formattersConfig))
	for _, formatterConfig := range formattersConfig {
		var formatterPlugin Plugin
		if err := json.Unmarshal(formatterConfig, &formatterPlugin); err != nil {
			log.Fatal(err)
		}
		newFormatFn, ok := formatters.Formatters[formatterPlugin.Type]
		if !ok {
			log.Fatalf("No such shipper %q", formatterPlugin.Type)
		}
		formatFn, err := newFormatFn(formatterConfig)
		if err != nil {
			log.Fatal(err)
		}
		formatFns = append(formatFns, formatFn)
	}

	transformer, err := transformer.NewTransformer(transformerConfig, formatFns, newOutputChunk)
	if err != nil {
		log.Fatal(err)
	}
	return transformer
}

func configureReader(readerConfig json.RawMessage) *reader.Reader {
	reader, err := reader.NewReader(readerConfig)
	if err != nil {
		log.Fatal(err)
	}
	return reader
}
