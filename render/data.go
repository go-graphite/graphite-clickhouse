package render

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"unsafe"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func reversePath(path string) string {
	// don't reverse tagged path
	if strings.IndexByte(path, '?') >= 0 {
		return path
	}

	a := strings.Split(path, ".")

	l := len(a)
	for i := 0; i < l/2; i++ {
		a[i], a[l-i-1] = a[l-i-1], a[i]
	}

	return strings.Join(a, ".")
}

var errUvarintRead = errors.New("ReadUvarint: Malformed array")
var errUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var errClickHouseResponse = errors.New("Malformed response from clickhouse")

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
	Aliases map[string][]string
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

// DataSplitFunc is split function for bufio.Scanner for read row binary records with data
func DataSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 && atEOF {
		// stop
		return 0, nil, nil
	}
	namelen, readBytes, err := ReadUvarint(data)
	if err == errUvarintRead {
		if atEOF {
			return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error(), string(data))
		}
		// signal for read more
		return 0, nil, nil
	}

	if err != nil {
		return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error(), string(data))
	}

	tokenLen := int(readBytes) + int(namelen) + 16

	if len(data) < tokenLen {
		if atEOF {
			return 0, nil, clickhouse.NewErrDataParse(errClickHouseResponse.Error(), string(data))
		}
		// signal for read more
		return 0, nil, nil
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
	scanner.Buffer(make([]byte, 1048576), 1048576)
	scanner.Split(DataSplitFunc)

	var row_start []byte

	for scanner.Scan() {
		row_start = scanner.Bytes()

		d.length += len(row_start)

		namelen, readBytes, err := ReadUvarint(row_start)
		if err != nil {
			return nil, errClickHouseResponse
		}

		row := row_start[int(readBytes):]

		newName := row[:int(namelen)]
		row = row[int(namelen):]

		if bytes.Compare(newName, name) != 0 {
			if len(newName) > len(nameBuf) {
				name = make([]byte, len(newName))
				copy(name, newName)
			} else {
				copy(nameBuf, newName)
				name = nameBuf[:len(newName)]
			}
			if isReverse {
				metricID = pp.MetricID(reversePath(string(name)))
			} else {
				metricID = pp.MetricID(string(name))
			}
		}

		time := binary.LittleEndian.Uint32(row[:4])
		row = row[4:]

		value := math.Float64frombits(binary.LittleEndian.Uint64(row[:8]))
		row = row[8:]

		timestamp := binary.LittleEndian.Uint32(row[:4])

		pp.AppendPoint(metricID, value, time, timestamp)
	}

	err := scanner.Err()
	if err != nil {
		dataErr, ok := err.(*clickhouse.ErrDataParse)
		if ok {
			// format full error string
			dataErr.PrependDescription(string(row_start))
		}
	}
	return d, err
}
