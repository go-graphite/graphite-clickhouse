package render

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
)

var errUvarintRead = errors.New("ReadUvarint: Malformed array")
var errUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var errClickHouseResponse = errors.New("Malformed response from clickhouse")

// QUERY to get data from ClickHouse
const QUERY = `SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp) FROM %s %s %s GROUP BY Path FORMAT RowBinary`

func ReadUvarint(array []byte) (uint64, int, error) {
	var x uint64
	var s uint
	l := len(array) - 1
	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, errUvarintRead
		}
		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, errUvarintOverflow
			}
			return x | uint64(array[i])<<s, i + 1, nil
		}
		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}

type Data struct {
	//body    []byte // raw RowBinary from clickhouse
	length  int // readed bytes count
	Points  *point.Points
	nameMap map[string]string
	Aliases *alias.Map
}

var EmptyData *Data = &Data{Points: point.NewPoints()}

func (d *Data) finalName(name string) string {
	s, ok := d.nameMap[name]
	if !ok {
		d.nameMap[name] = name
		return name
	}
	return s
}

// Error handler for DataSplitFunc
func splitErrorHandler(data *[]byte, atEOF bool, tokenLen int, err error) (int, []byte, error) {
	if err == errUvarintRead {
		if atEOF {
			return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error(), string(*data))
		}
		// signal for read more
		return 0, nil, nil
	} else if err != nil || (len(*data) < tokenLen && atEOF) {
		return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error(), string(*data))
	}
	// signal for read more
	return 0, nil, nil
}

// DataSplitFunc is split function for bufio.Scanner for read row binary records with data
func DataSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 && atEOF {
		// stop
		return 0, nil, nil
	}

	nameLen, readBytes, err := ReadUvarint(data)
	tokenLen := int(readBytes) + int(nameLen)
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	timeLen, readBytes, err := ReadUvarint(data[tokenLen:])
	tokenLen += int(readBytes) + int(timeLen)*4
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	valueLen, readBytes, err := ReadUvarint(data[tokenLen:])
	tokenLen += int(readBytes) + int(valueLen)*8
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	timestampLen, readBytes, err := ReadUvarint(data[tokenLen:])
	tokenLen += int(readBytes) + int(timestampLen)*4
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	if !(timeLen == valueLen && timeLen == timestampLen) {
		return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error()+": Different amount of Values, Times and Timestamps", string(data))
	}

	return tokenLen, data[:tokenLen], nil
}

func DataParse(bodyReader io.Reader, extraPoints *point.Points, isReverse bool) (*Data, error) {
	d := &Data{
		Points: point.NewPoints(),
	}

	pp := d.Points

	// add extraPoints. With NameToID
	if extraPoints != nil {
		extraList := extraPoints.List()
		for i := 0; i < len(extraList); i++ {
			pp.AppendPoint(
				pp.MetricID(extraPoints.MetricName(extraList[i].MetricID)),
				extraList[i].Value,
				extraList[i].Time,
				extraList[i].Timestamp,
			)
		}
	}

	nameBuf := make([]byte, 65536)
	name := []byte{}
	var metricID uint32

	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 10485760), 10485760)
	scanner.Split(DataSplitFunc)

	var rowStart []byte

	for scanner.Scan() {
		rowStart = scanner.Bytes()

		d.length += len(rowStart)

		nameLen, readBytes, err := ReadUvarint(rowStart)
		if err != nil {
			return nil, errClickHouseResponse
		}

		row := rowStart[int(readBytes):]

		newName := row[:int(nameLen)]
		row = row[int(nameLen):]

		if bytes.Compare(newName, name) != 0 {
			if len(newName) > len(nameBuf) {
				name = make([]byte, len(newName))
				copy(name, newName)
			} else {
				copy(nameBuf, newName)
				name = nameBuf[:len(newName)]
			}
			if isReverse {
				metricID = pp.MetricIDBytes(reverse.Bytes(name))
			} else {
				metricID = pp.MetricIDBytes(name)
			}
		}

		arrayLen, readBytes, err := ReadUvarint(row)
		if err != nil {
			return nil, errClickHouseResponse
		}

		times := make([]uint32, arrayLen)
		values := make([]float64, arrayLen)
		timestamps := make([]uint32, arrayLen)

		row = row[int(readBytes):]
		for i := uint64(0); i < arrayLen; i++ {
			times[i] = binary.LittleEndian.Uint32(row[:4])
			row = row[4:]
		}

		row = row[int(readBytes):]
		for i := uint64(0); i < arrayLen; i++ {
			values[i] = math.Float64frombits(binary.LittleEndian.Uint64(row[:8]))
			row = row[8:]
		}

		row = row[int(readBytes):]
		for i := uint64(0); i < arrayLen; i++ {
			timestamps[i] = binary.LittleEndian.Uint32(row[:4])
			row = row[4:]
		}

		for i := range times {
			pp.AppendPoint(metricID, values[i], times[i], timestamps[i])
		}
	}

	err := scanner.Err()
	if err != nil {
		dataErr, ok := err.(*clickhouse.ErrDataParse)
		if ok {
			// format full error string
			dataErr.PrependDescription(string(rowStart))
		}
	}
	return d, err
}
