package data

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
)

var errClickHouseResponse = errors.New("Malformed response from clickhouse")

// ReadUvarint reads unsigned int with variable length
var ReadUvarint = clickhouse.ReadUvarint

// Data stores parsed response from ClickHouse server
type Data struct {
	*point.Points
	AM         *alias.Map
	CommonStep int64
}

var emptyData *Data = &Data{Points: point.NewPoints(), AM: alias.New()}

func contextIsValid(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// GetStep returns the commonStep for all points or, if unset, step for metric ID id
func (d *Data) GetStep(id uint32) (uint32, error) {
	if 0 < d.CommonStep {
		return uint32(d.CommonStep), nil
	}
	return d.Points.GetStep(id)
}

// GetAggregation returns the generic whisper compatible name for an aggregation of metric with ID id
func (d *Data) GetAggregation(id uint32) (string, error) {
	function, err := d.Points.GetAggregation(id)
	if err != nil {
		return function, err
	}
	switch function {
	case "any":
		return "first", nil
	case "anyLast":
		return "last", nil
	default:
		return function, nil
	}
}

// data wraps Data and adds asynchronous processing of data
// data.wait() should be used with the same context as prepareData and parseResponse to check the error
type data struct {
	*Data
	length int           // readed bytes count
	spent  time.Duration // time spent on parsing
	b      chan io.ReadCloser
	e      chan error
	mut    sync.RWMutex
	wg     sync.WaitGroup
}

// prepareData returns new data with asynchronous processing points from carbonlinkClient
func prepareData(ctx context.Context, targets int, fetcher func() *point.Points) *data {
	data := &data{
		Data: &Data{Points: point.NewPoints()},
		b:    make(chan io.ReadCloser, 1),
		e:    make(chan error, targets),
		mut:  sync.RWMutex{},
		wg:   sync.WaitGroup{},
	}
	data.wg.Add(1)

	extraPoints := make(chan *point.Points, 1)

	go func() {
		// add extraPoints. With NameToID
		defer func() {
			data.wg.Done()
			close(extraPoints)
		}()

		// First check is context is already done
		if err := contextIsValid(ctx); err != nil {
			data.e <- fmt.Errorf("prepareData failed: %w", err)
			return
		}

		select {
		case extraPoints <- fetcher():
			p := <-extraPoints
			if p != nil {
				data.mut.Lock()
				defer data.mut.Unlock()

				extraList := p.List()
				for i := 0; i < len(extraList); i++ {
					data.Points.AppendPoint(
						data.Points.MetricID(p.MetricName(extraList[i].MetricID)),
						extraList[i].Value,
						extraList[i].Time,
						extraList[i].Timestamp,
					)
				}
			}
			return
		case <-ctx.Done():
			data.e <- fmt.Errorf("prepareData failed: %w", ctx.Err())
			return
		}
	}()
	return data
}

// setSteps sets commonStep for aggregated requests and per-metric step for non-aggregated
func (d *data) setSteps(cond *conditions) {
	if cond.aggregated {
		d.CommonStep = cond.step
		return
	}
	d.Points.SetSteps(cond.steps)
}

// Error handler for data splitting functions
func splitErrorHandler(data *[]byte, atEOF bool, tokenLen int, err error) (int, []byte, error) {
	if err == clickhouse.ErrUvarintRead {
		if atEOF {
			return 0, nil, clickhouse.NewErrWithDescr(errClickHouseResponse.Error(), string(*data))
		}
		// signal for read more
		return 0, nil, nil
	} else if err != nil || (len(*data) < tokenLen && atEOF) {
		return 0, nil, clickhouse.NewErrWithDescr(errClickHouseResponse.Error(), string(*data))
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
	tokenLen += int(readBytes) + int(timeLen)*4
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	valueLen, readBytes, err := ReadUvarint(data[tokenLen:])
	tokenLen += int(readBytes) + int(valueLen)*8
	if err != nil || len(data) < tokenLen {
		return splitErrorHandler(&data, atEOF, tokenLen, err)
	}

	if timeLen != valueLen {
		return 0, nil, clickhouse.NewErrWithDescr(errClickHouseResponse.Error()+": Different amount of Times and Values", string(data))
	}

	return tokenLen, data[:tokenLen], nil
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

	if timeLen != valueLen || timeLen != timestampLen {
		return 0, nil, clickhouse.NewErrWithDescr(errClickHouseResponse.Error()+": Different amount of Values, Times and Timestamps", string(data))
	}

	return tokenLen, data[:tokenLen], nil
}

// readResponse reads the ClickHouse body into *Data and merges with extraPoints.
// Expected, that on error the context will be cancelled on the upper level.
func (d *data) parseResponse(ctx context.Context, bodyReader io.ReadCloser, cond *conditions) error {
	pp := d.Points
	dataSplit := dataSplitUnaggregated
	if cond.aggregated {
		dataSplit = dataSplitAggregated
	}

	// Prevent starting parser if context is done
	if err := contextIsValid(ctx); err != nil {
		return ctx.Err()
	}

	// Then wait if there is an active parser working
	select {
	case d.b <- bodyReader:
	case <-ctx.Done():
		return fmt.Errorf("parseResponse failed: %w", ctx.Err())
	}

	var metricID uint32
	d.mut.Lock()
	defer func() {
		d.mut.Unlock()
		<-d.b
	}()

	// Are we still good to go?
	if err := contextIsValid(ctx); err != nil {
		return fmt.Errorf("parseResponse failed: %w", ctx.Err())
	}

	start := time.Now()
	scanner := bufio.NewScanner(bodyReader)
	scanner.Buffer(make([]byte, 1048576), 67108864)
	scanner.Split(dataSplit)

	var rowStart []byte
	for scanner.Scan() {
		rowStart = scanner.Bytes()

		d.length += len(rowStart)

		nameLen, readBytes, err := ReadUvarint(rowStart)
		if err != nil {
			return errClickHouseResponse
		}

		row := rowStart[int(readBytes):]

		name := row[:int(nameLen)]
		row = row[int(nameLen):]

		if cond.isReverse {
			metricID = pp.MetricIDBytes(reverse.Bytes(name))
		} else {
			metricID = pp.MetricIDBytes(name)
		}

		arrayLen, readBytes, err := ReadUvarint(row)
		if err != nil {
			return errClickHouseResponse
		}

		times := make([]uint32, 0, arrayLen)
		values := make([]float64, 0, arrayLen)

		row = row[int(readBytes):]
		for i := uint64(0); i < arrayLen; i++ {
			times = append(times, binary.LittleEndian.Uint32(row[:4]))
			row = row[4:]
		}

		row = row[int(readBytes):]
		for i := uint64(0); i < arrayLen; i++ {
			values = append(values, math.Float64frombits(binary.LittleEndian.Uint64(row[:8])))
			row = row[8:]
		}

		timestamps := times
		if !cond.aggregated {
			timestamps = make([]uint32, 0, arrayLen)
			row = row[int(readBytes):]
			for i := uint64(0); i < arrayLen; i++ {
				timestamps = append(timestamps, binary.LittleEndian.Uint32(row[:4]))
				row = row[4:]
			}
		}

		for i := range times {
			pp.AppendPoint(metricID, values[i], times[i], timestamps[i])
		}
	}
	d.spent += time.Since(start)

	err := scanner.Err()
	if err != nil {
		dataErr, ok := err.(*clickhouse.ErrWithDescr)
		if ok {
			// format full error string, sometimes parse not failed at start orf error string
			dataErr.PrependDescription(string(rowStart))
		}
		bodyReader.Close()
		return err
	}

	return nil
}

func (d *data) wait(ctx context.Context) error {
	// First check is context is already done
	if err := contextIsValid(ctx); err != nil {
		return fmt.Errorf("prepareData failed: %w", err)
	}

	// if anything is already in error channel
	select {
	case err := <-d.e:
		return err
	default:
	}

	// watch if workers are done
	parsersDone := make(chan struct{}, 1)
	go func() {
		d.wg.Wait()
		parsersDone <- struct{}{}
	}()

	// and watch for all channels
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-d.e:
		return err
	case <-parsersDone:
		return nil
	}
}
