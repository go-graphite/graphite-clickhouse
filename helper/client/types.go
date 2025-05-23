package client

import (
	"errors"
	"fmt"
)

type FormatType int

const (
	FormatDefault FormatType = iota
	FormatJSON
	FormatProtobuf
	FormatPb_v2 // alias for FormatProtobuf
	FormatPb_v3
	FormatPickle
)

var formatStrings []string = []string{"default", "json", "protobuf", "carbonapi_v2_pb", "carbonapi_v3_pb", "pickle"}

func (a *FormatType) String() string {
	return formatStrings[*a]
}

func FormatTypes() []string {
	return formatStrings
}

func (a *FormatType) Set(value string) error {
	switch value {
	case "json":
		*a = FormatJSON
	case "protobuf":
		*a = FormatProtobuf
	case "carbonapi_v2_pb":
		*a = FormatPb_v2
	case "carbonapi_v3_pb":
		*a = FormatPb_v3
	case "pickle":
		*a = FormatPickle
	default:
		return fmt.Errorf("invalid format type %s", value)
	}

	return nil
}

func (a *FormatType) UnmarshalText(text []byte) error {
	return a.Set(string(text))
}

var (
	ErrUnsupportedFormat = errors.New("unsupported format")
	ErrInvalidQuery      = errors.New("invalid query")

	//ErrEmptyQuery = errors.New("missing query")
)
