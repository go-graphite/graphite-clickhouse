package render

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unsafe"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func reversePath(path string) string {
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
	body    []byte // raw RowBinary from clickhouse
	Points  []point.Point
	nameMap map[string]string
	Aliases map[string][]string
}

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
			return 0, nil, errClickHouseResponse
		}
		// signal for read more
		return 0, nil, nil
	}

	if err != nil {
		return 0, nil, err
	}

	tokenLen := int(readBytes) + int(namelen) + 16

	if len(data) < tokenLen {
		if atEOF {
			return 0, nil, errClickHouseResponse
		}
		// signal for read more
		return 0, nil, nil
	}

	return tokenLen, data[:tokenLen], nil
}

func DataParse(bodyReader io.Reader, extraPoints []point.Point, isReverse bool) (*Data, error) {

	d := &Data{
		Points:  make([]point.Point, 0, len(extraPoints)),
		nameMap: make(map[string]string),
	}

	var p point.Point

	// add extraPoints. With NameToID
	for i := 0; i < len(extraPoints); i++ {
		extraPoints[i].Metric = d.finalName(extraPoints[i].Metric)
		d.Points = append(d.Points, extraPoints[i])
	}

	nameBuf := make([]byte, 65536)
	name := []byte{}
	finalName := ""

	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 1048576), 1048576)
	scanner.Split(DataSplitFunc)

	for scanner.Scan() {
		row := scanner.Bytes()

		namelen, readBytes, err := ReadUvarint(row)
		if err != nil {
			return nil, errClickHouseResponse
		}
		row = row[int(readBytes):]

		newName := row[:int(namelen)]
		row = row[int(namelen):]

		fmt.Println("cmp", string(name), string(newName))
		if bytes.Compare(newName, name) != 0 {
			if len(newName) > len(nameBuf) {
				name = make([]byte, len(newName))
				copy(name, newName)
			} else {
				copy(nameBuf, newName)
				name = nameBuf[:len(newName)]
			}
			if isReverse {
				finalName = d.finalName(reversePath(string(name)))
			} else {
				finalName = d.finalName(string(name))
			}
		}

		time := binary.LittleEndian.Uint32(row[:4])
		row = row[4:]

		value := math.Float64frombits(binary.LittleEndian.Uint64(row[:8]))
		row = row[8:]

		timestamp := binary.LittleEndian.Uint32(row[:4])

		p.Metric = finalName
		p.Time = int32(time)
		p.Value = value
		p.Timestamp = int32(timestamp)
		d.Points = append(d.Points, p)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *Data) Len() int {
	return len(d.Points)
}

func (d *Data) Less(i, j int) bool {
	if d.Points[i].Metric == d.Points[j].Metric {
		return d.Points[i].Time < d.Points[j].Time
	}

	return d.Points[i].Metric < d.Points[j].Metric
}

func (d *Data) Swap(i, j int) {
	d.Points[i], d.Points[j] = d.Points[j], d.Points[i]
}
