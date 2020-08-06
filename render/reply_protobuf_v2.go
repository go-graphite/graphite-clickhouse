package render

import (
	"bufio"
	"bytes"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

func writePB2(mb, mb2 *bytes.Buffer, writer *bufio.Writer, target, name string, from, until, step uint32, points []point.Point) {
	start := from - (from % step)
	if start < from {
		start += step
	}
	stop := until - (until % step) + step
	count := (stop - start) / step

	mb.Reset()

	// name
	VarintWrite(mb, (1<<3)+2) // tag
	VarintWrite(mb, uint64(len(name)))
	mb.WriteString(name)

	// start
	VarintWrite(mb, 2<<3)
	VarintWrite(mb, uint64(start))

	// stop
	VarintWrite(mb, 3<<3)
	VarintWrite(mb, uint64(stop))

	// step
	VarintWrite(mb, 4<<3)
	VarintWrite(mb, uint64(step))

	// start write to output

	// repeated FetchResponse metrics = 1;
	// write tag and len
	VarintWrite(writer, (1<<3)+2)
	VarintWrite(writer,
		uint64(mb.Len())+
			2+ // tags of <repeated double values = 5;> and <repeated bool isAbsent = 6;>
			VarintLen(uint64(8*count))+ // len of packed <repeated double values>
			VarintLen(uint64(count))+ // len of packed <repeated bool isAbsent>
			uint64(9*count), // packed <repeated double values> and <repeated bool isAbsent>
	)

	writer.Write(mb.Bytes())

	// Write values
	VarintWrite(writer, (5<<3)+2)
	VarintWrite(writer, uint64(8*count))

	last := start - step
	for _, p := range points {
		if p.Time < start || p.Time >= stop {
			continue
		}

		if p.Time > last+step {
			ProtobufWriteDoubleN(writer, 0, int(((p.Time-last)/step)-1))
		}

		ProtobufWriteDouble(writer, p.Value)

		last = p.Time
	}

	if stop-step > last {
		ProtobufWriteDoubleN(writer, 0, int(((stop-last)/step)-1))
	}

	// Write isAbsent
	VarintWrite(writer, (6<<3)+2)
	VarintWrite(writer, uint64(count))

	last = start - step
	for _, p := range points {
		if p.Time < start || p.Time >= stop {
			continue
		}

		if p.Time > last+step {
			WriteByteN(writer, '\x01', int(((p.Time-last)/step)-1))
		}

		writer.WriteByte('\x00')

		last = p.Time
	}

	if stop-step > last {
		WriteByteN(writer, '\x01', int(((stop-last)/step)-1))
	}
}
