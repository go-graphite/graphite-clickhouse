package backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"unsafe"
)

type Point struct {
	id        int
	Metric    string
	Time      int32
	Value     float64
	Timestamp int32 // keep max if metric and time equal on two points
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
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
	body     []byte // raw RowBinary from clickhouse
	Points   []Point
	nameToID map[string]int
	maxID    int
}

func (d *Data) NameToID(name string) int {
	id := d.nameToID[name]
	if id == 0 {
		d.maxID++
		id = d.maxID
		d.nameToID[name] = id
	}
	return id
}

func DataCount(body []byte) (int, error) {
	var namelen uint64
	bodyLen := len(body)
	var count, offset, readBytes int
	var err error

	for {
		if offset >= bodyLen {
			if offset == bodyLen {
				return count, nil
			}
			return 0, errClickHouseResponse
		}
		namelen, readBytes, err = ReadUvarint(body[offset:])
		if err != nil {
			return 0, err
		}
		offset += readBytes + int(namelen) + 16
		count++
	}

	return 0, nil
}

func DataParse(body []byte) (*Data, error) {
	count, err := DataCount(body)
	if err != nil {
		return nil, err
	}

	d := &Data{
		Points:   make([]Point, count),
		nameToID: make(map[string]int),
	}

	var namelen uint64
	offset := 0
	readBytes := 0
	bodyLen := len(body)
	index := 0

	name := []byte{}
	id := 0

	for {
		if offset >= bodyLen {
			if offset == bodyLen {
				break
			}
			return nil, errClickHouseResponse
		}

		namelen, readBytes, err = ReadUvarint(body[offset:])
		if err != nil {
			return nil, errClickHouseResponse
		}
		offset += readBytes

		if bodyLen-offset < int(namelen)+16 {
			return nil, errClickHouseResponse
		}

		newName := body[offset : offset+int(namelen)]
		offset += int(namelen)

		if bytes.Compare(newName, name) != 0 {
			name = newName
			id = d.NameToID(unsafeString(name))
			// fmt.Println(unsafeString(name), id)
		}

		time := binary.LittleEndian.Uint32(body[offset : offset+4])
		offset += 4

		value := math.Float64frombits(binary.LittleEndian.Uint64(body[offset : offset+8]))
		offset += 8

		timestamp := binary.LittleEndian.Uint32(body[offset : offset+4])
		offset += 4

		d.Points[index].id = id
		d.Points[index].Metric = unsafeString(name)
		d.Points[index].Time = int32(time)
		d.Points[index].Value = value
		d.Points[index].Timestamp = int32(timestamp)
		index++
	}

	return d, nil
}
