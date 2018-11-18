package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/ghodss/yaml"

	"github.com/wryun/journalship/internal/formatters"
	"github.com/wryun/journalship/internal/reader"
	"github.com/wryun/journalship/internal/shippers"
	"github.com/wryun/journalship/internal/transformer"
	"github.com/wryun/journalship/internal/writer"
)

type Config struct {
	NumTransformers int `json:"numTransformers"`
	NumShippers     int `json:"numShippers"`

	Reader json.RawMessage `json:"reader"`

	Transformer json.RawMessage   `json:"transformer"`
	Formatters  []json.RawMessage `json:"formatters"`

	Writer  json.RawMessage `json:"writer"`
	Shipper json.RawMessage `json:"shipper"`
}

type Plugin struct {
	Type string `json:"type"`
}

func main() {
	rand.Seed(time.Now().UnixNano())
	config := loadConfig()
	rdr := configureReader(config.Reader)
	writer := configureWriter(config.Writer)
	// We only ever have one shipper because we use journald as our
	// buffer and only want to track one cursor location.
	// If you want to ship to multiple upstreams, run multiple journalships.
	shipper := configureShipper(config.Shipper)
	transformer := configureTransformer(config.Transformer, config.Formatters, shipper.NewOutputChunk)

	inputChunksChannel := make(chan reader.InputChunk)
	outputChunksChannel := make(chan shippers.OutputChunk)

	for i := 0; i < config.NumTransformers; i++ {
		go transformer.Run(inputChunksChannel, rdr.CursorSaver, outputChunksChannel)
	}
	// TODO should really have a dynamically resizing pool here...
	// (otherwise)
	for i := 0; i < config.NumShippers; i++ {
		// TODO a new shipper
		go writer.Run(shipper.Instance(), outputChunksChannel, rdr.CursorSaver)
	}

	// It only makes sense to have one reader due to how journald works.
	// (unless we were doing something super complex...)
	// Also, unlike the coreos journald library, we don't use mutexes
	// on the C calls (i.e. it's not thread safe).
	rdr.Run(inputChunksChannel)
}

func loadConfig() Config {
	configFileName := flag.String("c", "", "file to use for config")
	flag.Parse()

	if *configFileName == "" {
		log.Fatal("must specify config file (-c)")
	}

	configFile, err := ioutil.ReadFile(*configFileName)
	if err != nil {
		log.Fatal(err)
	}

	config := Config{
		NumShippers:     2,
		NumTransformers: 2,
		Transformer:     []byte("{}"),
		Reader:          []byte("{}"),
		Shipper:         []byte("{}"),
		Writer:          []byte("{}"),
		Formatters:      []json.RawMessage{},
	}
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
	if shipperPlugin.Type == "" {
		log.Fatal("must specify type of shipper to use")
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

func configureWriter(writerConfig json.RawMessage) *writer.Writer {
	writer, err := writer.NewWriter(writerConfig)
	if err != nil {
		log.Fatal(err)
	}
	return writer
}
