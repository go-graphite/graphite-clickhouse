package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

type CompAlgo int

const (
	CompAlgoNone CompAlgo = iota
	CompAlgoLZ4
)

var compressionMap = map[string]CompAlgo{
	"none": CompAlgoNone,
	"lz4":  CompAlgoLZ4,
}

var compressionMapReversed = map[CompAlgo]string{}

func init() {
	for k, v := range compressionMap {
		compressionMapReversed[v] = k
	}
}

// Compression wrapper for TOML
type Compression struct {
	CompAlgo
}

var _ toml.TextMarshaler = &Compression{}

// UnmarshalText from TOML
func (c *Compression) UnmarshalText(text []byte) (err error) {
	t := string(text)
	algo, ok := compressionMap[t]
	if !ok {
		return fmt.Errorf("Compression algorithm '%s' not supported", t)
	}

	c.CompAlgo = algo
	return
}

// MarshalText encode text with TOML format
func (c *Compression) MarshalText() ([]byte, error) {
	return []byte(compressionMapReversed[c.CompAlgo]), nil
}

// Value return time.Duration value
func (c *Compression) Value() CompAlgo {
	return c.CompAlgo
}
