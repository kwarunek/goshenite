// parser
package main

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"unsafe"
)

type DataPoint struct {
	Metric    string
	Value     float64
	Timestamp int64
}

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func PlainLine(p []byte) ([]byte, float64, int64, error) {
	p = bytes.Trim(p, " \n\r")

	i1 := bytes.IndexByte(p, ' ')
	if i1 < 1 {
		return nil, 0, 0, errors.New("bad_message")
	}

	i2 := bytes.IndexByte(p[i1+1:], ' ')
	if i2 < 1 {
		return nil, 0, 0, errors.New("bad_message")
	}
	i2 += i1 + 1

	i3 := len(p)

	value, err := strconv.ParseFloat(unsafeString(p[i1+1:i2]), 64)
	if err != nil || math.IsNaN(value) {
		//return nil, 0, 0, fmt.Errorf("bad_message: %#v", string(p))
		return nil, 0, 0, errors.New("bad_message")
	}

	tsf, err := strconv.ParseFloat(unsafeString(p[i2+1:i3]), 64)
	if err != nil || math.IsNaN(tsf) {
		return nil, 0, 0, errors.New("bad_message")
	}

	return p[:i1], value, int64(tsf), nil
}

func ParsePlainGraphiteProtocol(body []byte) ([]DataPoint, error) {
	var result []DataPoint

	size := len(body)
	offset := 0

MainLoop:
	for offset < size {
		lineEnd := bytes.IndexByte(body[offset:size], '\n')
		if lineEnd < 0 {
			return result, errors.New("unfinished_line")
		} else if lineEnd == 0 {
			// skip empty line
			offset++
			continue MainLoop
		}

		name, value, timestamp, err := PlainLine(body[offset : offset+lineEnd+1])
		offset += lineEnd + 1

		if err != nil {
			return result, err
		}

		dp := DataPoint{Metric: string(name), Value: value, Timestamp: timestamp}
		result = append(result, dp)
	}

	return result, nil
}
