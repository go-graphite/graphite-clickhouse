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
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
)

var errClickHouseResponse = errors.New("Malformed response from clickhouse")

var ReadUvarint = clickhouse.ReadUvarint

type Data struct {
	length     int // readed bytes count
	Points     *point.Points
	Aliases    *alias.Map
	rollupObj  *rollup.Rules
	commonStep int64
}

var EmptyData *Data = &Data{Points: point.NewPoints()}

func prepare(extraPoints *point.Points) *Data {
	data := &Data{
		Points: point.NewPoints(),
	}

	// add extraPoints. With NameToID
	if extraPoints != nil {
		extraList := extraPoints.List()
		for i := 0; i < len(extraList); i++ {
			data.Points.AppendPoint(
				data.Points.MetricID(extraPoints.MetricName(extraList[i].MetricID)),
				extraList[i].Value,
				extraList[i].Time,
				extraList[i].Timestamp,
			)
		}
	}
	return data
}

// GetStep returns the step for metric ID i
func (d *Data) GetStep(id uint32) (uint32, error) {
	if 0 < d.commonStep {
		return uint32(d.commonStep), nil
	}
	return d.Points.GetStep(id)
}

// Error handler for data splitting functions
func splitErrorHandler(data *[]byte, atEOF bool, tokenLen int, err error) (int, []byte, error) {
	if err == clickhouse.ErrUvarintRead {
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

// dataSplitAggregated is a split function for bufio.Scanner for read row binary response for queries of aggregated data
func dataSplitAggregated(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
	tokenLen += int(readBytes) + int(timeLen)*5
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	valueLen, readBytes, err := ReadUvarint(data[tokenLen:])
	tokenLen += int(readBytes) + int(valueLen)*9
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	if !(timeLen == valueLen) {
		return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error()+": Different amount of Times and Values", string(data))
	}

	return tokenLen, data[:tokenLen], nil
}

// parseAggregatedResponse reads the ClickHouse body into *Data and merges with extraPoints
func parseAggregatedResponse(b chan io.ReadCloser, e chan error, extraPoints *point.Points, isReverse bool) (*Data, error) {
	d := prepare(extraPoints)
	pp := d.Points

	nameBuf := make([]byte, 65536)
	name := []byte{}
	var metricID uint32

	for r := 1; r <= cap(b); r++ {
		select {
		case err := <-e:
			return d, err
		case bodyReader := <-b:
			scanner := bufio.NewScanner(bodyReader)
			scanner.Buffer(make([]byte, 10485760), 10485760)
			scanner.Split(dataSplitAggregated)

			var rowStart []byte
			for scanner.Scan() {
				rowStart = scanner.Bytes()

				d.length += len(rowStart)

				nameLen, readBytes, err := ReadUvarint(rowStart)
				if err != nil {
					return d, errClickHouseResponse
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
					return d, errClickHouseResponse
				}

				times := make([]uint32, 0, arrayLen)
				values := make([]float64, 0, arrayLen)

				row = row[int(readBytes):]
				for i := uint64(0); i < arrayLen; i++ {
					times = append(times, binary.LittleEndian.Uint32(row[1:5]))
					row = row[5:]
				}

				row = row[int(readBytes):]
				for i := uint64(0); i < arrayLen; i++ {
					values = append(values, math.Float64frombits(binary.LittleEndian.Uint64(row[1:9])))
					row = row[9:]
				}

				for i := range times {
					pp.AppendPoint(metricID, values[i], times[i], times[i])
				}
			}
			err := scanner.Err()
			if err != nil {
				dataErr, ok := err.(*clickhouse.ErrDataParse)
				if ok {
					// format full error string
					dataErr.PrependDescription(string(rowStart))
				}
				bodyReader.Close()
				return d, err
			}
		}
	}

	return d, nil
}

// dataSplitUnaggregated is a split function for bufio.Scanner for read row binary response for queries of unaggregated data
func dataSplitUnaggregated(data []byte, atEOF bool) (advance int, token []byte, err error) {
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

// parseUnaggregatedResponse reads the ClickHouse body into *Data and merges with extraPoints
func parseUnaggregatedResponse(bodyReader io.Reader, extraPoints *point.Points, isReverse bool) (*Data, error) {
	d := prepare(extraPoints)
	pp := d.Points

	nameBuf := make([]byte, 65536)
	name := []byte{}
	var metricID uint32

	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 10485760), 10485760)
	scanner.Split(dataSplitUnaggregated)

	var rowStart []byte

	for scanner.Scan() {
		rowStart = scanner.Bytes()

		d.length += len(rowStart)

		nameLen, readBytes, err := ReadUvarint(rowStart)
		if err != nil {
			return d, errClickHouseResponse
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
			return d, errClickHouseResponse
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
