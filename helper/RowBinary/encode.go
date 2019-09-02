package RowBinary

import (
	"encoding/binary"
	"io"
	"math"
	"time"
)

func DateToUint16(t time.Time) uint16 {
	return uint16(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

type Encoder struct {
	wrapped io.Writer
	buffer  []byte
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		wrapped: w,
		buffer:  make([]byte, 256),
	}
}

func (w *Encoder) Date(value time.Time) error {
	return w.Uint16(DateToUint16(value))
}

func (w *Encoder) Uint8(value uint8) error {
	_, err := w.wrapped.Write([]byte{value})
	return err
}

func (w *Encoder) Uint16(value uint16) error {
	binary.LittleEndian.PutUint16(w.buffer, value)
	_, err := w.wrapped.Write(w.buffer[:2])
	return err
}

func (w *Encoder) Uint32(value uint32) error {
	binary.LittleEndian.PutUint32(w.buffer, value)
	_, err := w.wrapped.Write(w.buffer[:4])
	return err
}

func (w *Encoder) Uint64(value uint64) error {
	binary.LittleEndian.PutUint64(w.buffer, value)
	_, err := w.wrapped.Write(w.buffer[:8])
	return err
}

func (w *Encoder) Float64(value float64) error {
	return w.Uint64(math.Float64bits(value))
}

func (w *Encoder) Bytes(value []byte) error {
	n := binary.PutUvarint(w.buffer, uint64(len(value)))
	_, err := w.wrapped.Write(w.buffer[:n])
	if err != nil {
		return err
	}

	_, err = w.wrapped.Write(value)
	return err
}

func (w *Encoder) String(value string) error {
	return w.Bytes([]byte(value))
}

func (w *Encoder) StringList(value []string) error {
	n := binary.PutUvarint(w.buffer, uint64(len(value)))
	_, err := w.wrapped.Write(w.buffer[:n])
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.String(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Encoder) Uint32List(value []uint32) error {
	n := binary.PutUvarint(w.buffer, uint64(len(value)))
	_, err := w.wrapped.Write(w.buffer[:n])
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.Uint32(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Encoder) Float64List(value []float64) error {
	n := binary.PutUvarint(w.buffer, uint64(len(value)))
	_, err := w.wrapped.Write(w.buffer[:n])
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.Float64(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}
