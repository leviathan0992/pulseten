// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

/*
#include <stdlib.h>

#include "shim.h"
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

// Tag is a VARCHAR column with the tag kind.
type Tag struct {
	// Name is the column name, matched case-insensitively.
	Name string

	// Value is the tag value, UTF-8.
	Value string
}

// Field is a BOOLEAN, BIGINT, DOUBLE or VARCHAR column. Value is a bool;
// int, int8, int16, int32, int64, uint, uint8, uint16, uint32 or uint64;
// float32 or float64; or string. Unsigned inputs must fit a signed 64-bit
// integer.
type Field struct {
	// Name is the column name, matched case-insensitively.
	Name string

	// Value is the field value.
	Value any
}

// Point is a row of a table. Columns it does not name are NULL.
type Point struct {
	// Time is the point's instant in [1900-01-01, 2200-01-01) UTC,
	// stored with microsecond precision.
	Time time.Time

	// Tags are the point's tags; may be empty.
	Tags []Tag

	// Fields are the point's fields, at least one.
	Fields []Field
}

// inputValue is a Field.Value or a parameter converted for the C union.
type inputValue struct {
	// m_type is the type of the value.
	m_type C.pulseten_type_t

	// m_is_null marks a NULL parameter; the other members are then unused.
	m_is_null bool

	// m_integer holds a BOOLEAN as 0 or 1, a BIGINT, or a TIMESTAMP parameter
	// in microseconds.
	m_integer int64

	// m_double holds a DOUBLE.
	m_double float64

	// m_string holds a VARCHAR.
	m_string string
}

// convertValue converts the Go types that fields and parameters share. It
// returns false for another type, or an unsigned integer that is too large.
func convertValue(v any) (inputValue, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return inputValue{m_type: C.PULSETEN_TYPE_BOOLEAN, m_integer: 1}, true
		}

		return inputValue{m_type: C.PULSETEN_TYPE_BOOLEAN}, true

	case int:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case int8:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case int16:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case int32:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case int64:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: x}, true

	case uint:
		if uint64(x) > 1<<63-1 {
			break
		}

		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case uint8:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case uint16:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case uint32:
		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case uint64:
		if x > 1<<63-1 {
			break
		}

		return inputValue{m_type: C.PULSETEN_TYPE_BIGINT, m_integer: int64(x)}, true

	case float32:
		return inputValue{m_type: C.PULSETEN_TYPE_DOUBLE, m_double: float64(x)}, true

	case float64:
		return inputValue{m_type: C.PULSETEN_TYPE_DOUBLE, m_double: x}, true

	case string:
		return inputValue{m_type: C.PULSETEN_TYPE_VARCHAR, m_string: x}, true
	}

	return inputValue{}, false
}

// convertFieldValue converts a field value; p and f locate it for the error.
func convertFieldValue(v any, p, f int) (inputValue, error) {
	value, ok := convertValue(v)
	if !ok {
		return inputValue{}, &Error{
			Code: CodeInvalidArgument,
			Msg:  fmt.Sprintf("point %d field %d: unsupported value of type %T", p, f, v),
		}
	}

	return value, nil
}

// arena is one C allocation holding a whole batch, since cgo does not let C
// keep Go pointers inside C structs.
type arena struct {
	// m_base is the allocation.
	m_base unsafe.Pointer

	// m_used_bytes is the bytes handed out so far.
	m_used_bytes uintptr
}

// alloc hands out n bytes aligned for any C type.
func (a *arena) alloc(n uintptr) unsafe.Pointer {
	a.m_used_bytes = (a.m_used_bytes + 7) &^ 7
	p := unsafe.Add(a.m_base, a.m_used_bytes)
	a.m_used_bytes += n

	return p
}

// copyString copies s into the arena and returns its C pointer.
func (a *arena) copyString(s string) *C.char {
	if len(s) == 0 {
		return nil
	}

	p := a.alloc(uintptr(len(s)))
	copy(unsafe.Slice((*byte)(p), len(s)), s)

	return (*C.char)(p)
}

// storeValue fills a C value union. The union is raw bytes to Go: an int, an
// int64, a double, or a pointer and a length.
func (a *arena) storeValue(u unsafe.Pointer, v *inputValue) {
	switch v.m_type {
	case C.PULSETEN_TYPE_BOOLEAN:
		*(*C.int)(u) = C.int(v.m_integer)
	case C.PULSETEN_TYPE_BIGINT, C.PULSETEN_TYPE_TIMESTAMP:
		*(*C.int64_t)(u) = C.int64_t(v.m_integer)
	case C.PULSETEN_TYPE_DOUBLE:
		*(*C.double)(u) = C.double(v.m_double)
	case C.PULSETEN_TYPE_VARCHAR:
		*(**C.char)(u) = a.copyString(v.m_string)
		*(*C.size_t)(unsafe.Add(u, unsafe.Sizeof(uintptr(0)))) =
			C.size_t(len(v.m_string))
	}
}

// writeArenaSize rejects inputs that cannot fit before allocating or copying
// their C representation. Schema-dependent validation remains in ingest.
func writeArenaSize(points []Point) (uintptr, error) {
	// The C counts are 32-bit; larger ones would wrap around before the
	// library could reject them.
	if len(points) > MaxPoints {
		return 0, &Error{
			Code: CodeLimitExceeded,
			Msg:  fmt.Sprintf("batch has more than %d points", MaxPoints),
		}
	}

	const maxBatchBytes = uint64(C.PULSETEN_MAX_BATCH_BYTES)
	var dataBytes uint64
	arenaBytes := uint64(len(points))*uint64(unsafe.Sizeof(C.pulseten_point_t{})) + 8

	for p := range points {
		pt := &points[p]
		if len(pt.Tags) >= MaxColumns || len(pt.Fields) >= MaxColumns-len(pt.Tags) {
			return 0, &Error{Code: CodeLimitExceeded, Msg: fmt.Sprintf("point %d: too many tags and fields", p)}
		}

		dataBytes += 8 * uint64(1+len(pt.Tags)+len(pt.Fields))
		arenaBytes += uint64(len(pt.Tags))*uint64(unsafe.Sizeof(C.pulseten_tag_t{})) + 8
		arenaBytes += uint64(len(pt.Fields))*uint64(unsafe.Sizeof(C.pulseten_field_t{})) + 8

		for _, tag := range pt.Tags {
			if len(tag.Name) > MaxNameBytes {
				return 0, &Error{Code: CodeInvalidArgument, Msg: "tag name is too long"}
			}

			if len(tag.Value) > MaxStringBytes {
				return 0, &Error{Code: CodeLimitExceeded, Msg: "tag value is too long"}
			}

			dataBytes += uint64(len(tag.Value))
			arenaBytes += uint64(len(tag.Name)) + uint64(len(tag.Value)) + 16
		}

		for _, field := range pt.Fields {
			if len(field.Name) > MaxNameBytes {
				return 0, &Error{Code: CodeInvalidArgument, Msg: "field name is too long"}
			}

			value, _ := field.Value.(string)
			if len(value) > MaxStringBytes {
				return 0, &Error{Code: CodeLimitExceeded, Msg: "field value is too long"}
			}

			dataBytes += uint64(len(value))
			arenaBytes += uint64(len(field.Name)) + uint64(len(value)) + 16
		}

		if dataBytes > maxBatchBytes {
			return 0, &Error{Code: CodeLimitExceeded, Msg: fmt.Sprintf("batch data exceeds %d bytes", maxBatchBytes)}
		}

		if arenaBytes > uint64(^uintptr(0)) {
			return 0, &Error{Code: CodeLimitExceeded, Msg: "C input allocation is too large"}
		}
	}

	return uintptr(arenaBytes), nil
}

// Write writes a batch of points to a table, creating the table and any new
// columns. The batch is accepted or rejected as a whole; once Write returns
// a nil error it is logged and visible to new queries. With WALSync enabled,
// it is also durable; the default does not wait for a WAL sync.
//
// Open Rows delay writes that add columns until Rows.Close. A nonzero
// Options.WriteLockTimeout bounds each table or namespace lock wait;
// ErrLockTimeout means nothing was logged and the write may safely be retried.
func (db *DB) Write(table string, points ...Point) error {
	n, err := writeArenaSize(points)
	if err != nil {
		return err
	}

	if len(table) > MaxNameBytes {
		return &Error{Code: CodeInvalidArgument, Msg: "table name is too long"}
	}

	ctable, err := checkedCString(table, "table name")
	if err != nil {
		return err
	}

	defer C.free(unsafe.Pointer(ctable))

	// Check the original instant before UnixMicro can overflow.
	minTime := time.UnixMicro(int64(C.PULSETEN_MIN_TIME_US))
	maxTime := time.UnixMicro(int64(C.PULSETEN_MAX_TIME_US))

	// Convert field values after the input's allocation size is known.
	values := make([][]inputValue, len(points))

	for p := range points {
		pt := &points[p]

		if pt.Time.Before(minTime) || !pt.Time.Before(maxTime) {
			return &Error{
				Code: CodeInvalidArgument,
				Msg:  fmt.Sprintf("point %d: time must be in [1900-01-01, 2200-01-01) UTC", p),
			}
		}

		values[p] = make([]inputValue, len(pt.Fields))

		for f := range pt.Fields {
			v, err := convertFieldValue(pt.Fields[f].Value, p, f)
			if err != nil {
				return err
			}

			values[p][f] = v
		}
	}

	a := arena{m_base: C.malloc(C.size_t(n))}
	if a.m_base == nil {
		return &Error{Code: CodeOutOfMemory}
	}

	defer C.free(a.m_base)

	// Fill the C points; strings are copied into the arena.
	var cpoints *C.pulseten_point_t

	if len(points) > 0 {
		cpoints = (*C.pulseten_point_t)(
			a.alloc(uintptr(len(points)) * unsafe.Sizeof(C.pulseten_point_t{})))
	}

	cps := unsafe.Slice(cpoints, len(points))

	for p := range points {
		pt := &points[p]
		cp := &cps[p]

		cp.m_time_us = C.int64_t(pt.Time.UnixMicro())
		cp.m_tag_count = C.uint32_t(len(pt.Tags))
		cp.m_field_count = C.uint32_t(len(pt.Fields))

		if len(pt.Tags) > 0 {
			cp.m_tags = (*C.pulseten_tag_t)(
				a.alloc(uintptr(len(pt.Tags)) * unsafe.Sizeof(C.pulseten_tag_t{})))
		}

		ctags := unsafe.Slice(cp.m_tags, len(pt.Tags))

		for t, tag := range pt.Tags {
			ctags[t].m_name = a.copyString(tag.Name)
			ctags[t].m_name_length = C.size_t(len(tag.Name))
			ctags[t].m_value = a.copyString(tag.Value)
			ctags[t].m_value_length = C.size_t(len(tag.Value))
		}

		if len(pt.Fields) > 0 {
			cp.m_fields = (*C.pulseten_field_t)(
				a.alloc(uintptr(len(pt.Fields)) * unsafe.Sizeof(C.pulseten_field_t{})))
		}

		cfields := unsafe.Slice(cp.m_fields, len(pt.Fields))

		for f := range pt.Fields {
			cf := &cfields[f]
			v := &values[p][f]

			cf.m_name = a.copyString(pt.Fields[f].Name)
			cf.m_name_length = C.size_t(len(pt.Fields[f].Name))
			cf.m_type = v.m_type

			a.storeValue(unsafe.Pointer(&cf.m_value[0]), v)
		}
	}

	h, release, err := db.acquireHandle()
	if err != nil {
		return err
	}

	defer release()

	var msg *C.char

	return toError(
		C.pt_go_write(h, ctable, cpoints, C.uint32_t(len(points)), &msg),
		msg)
}
