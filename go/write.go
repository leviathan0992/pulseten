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

// fieldValue is a Field.Value converted to the C union.
type fieldValue struct {
	// m_typ is the column type.
	m_typ C.pulseten_type_t

	// m_i holds a BOOLEAN as 0 or 1, or a BIGINT.
	m_i int64

	// m_d holds a DOUBLE.
	m_d float64

	// m_s holds a VARCHAR.
	m_s string
}

// convertValue converts a field value; p and f locate it for the error.
func convertValue(v any, p, f int) (fieldValue, error) {
	switch x := v.(type) {
	case bool:
		if x {
			return fieldValue{m_typ: C.PULSETEN_TYPE_BOOLEAN, m_i: 1}, nil
		}

		return fieldValue{m_typ: C.PULSETEN_TYPE_BOOLEAN}, nil
	case int:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case int8:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case int16:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case int32:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case int64:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: x}, nil
	case uint:
		if uint64(x) > 1<<63-1 {
			break
		}

		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case uint8:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case uint16:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case uint32:
		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case uint64:
		if x > 1<<63-1 {
			break
		}

		return fieldValue{m_typ: C.PULSETEN_TYPE_BIGINT, m_i: int64(x)}, nil
	case float32:
		return fieldValue{m_typ: C.PULSETEN_TYPE_DOUBLE, m_d: float64(x)}, nil
	case float64:
		return fieldValue{m_typ: C.PULSETEN_TYPE_DOUBLE, m_d: x}, nil
	case string:
		return fieldValue{m_typ: C.PULSETEN_TYPE_VARCHAR, m_s: x}, nil
	}

	return fieldValue{}, &Error{
		Code: CodeInvalidArgument,
		Msg:  fmt.Sprintf("point %d field %d: unsupported value of type %T", p, f, v),
	}
}

// arena is one C allocation holding a whole batch, since cgo does not let C
// keep Go pointers inside C structs.
type arena struct {
	// m_base is the allocation.
	m_base unsafe.Pointer

	// m_used is the bytes handed out so far.
	m_used uintptr
}

// alloc hands out n bytes aligned for any C type.
func (a *arena) alloc(n uintptr) unsafe.Pointer {
	a.m_used = (a.m_used + 7) &^ 7
	p := unsafe.Add(a.m_base, a.m_used)
	a.m_used += n

	return p
}

// str copies s into the arena and returns its C pointer.
func (a *arena) str(s string) *C.char {
	if len(s) == 0 {
		return nil
	}

	p := a.alloc(uintptr(len(s)))
	copy(unsafe.Slice((*byte)(p), len(s)), s)

	return (*C.char)(p)
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
			if len(tag.Name) > MaxNameLen {
				return 0, &Error{Code: CodeInvalidArgument, Msg: "tag name is too long"}
			}

			if len(tag.Value) > MaxStringLen {
				return 0, &Error{Code: CodeLimitExceeded, Msg: "tag value is too long"}
			}

			dataBytes += uint64(len(tag.Value))
			arenaBytes += uint64(len(tag.Name)) + uint64(len(tag.Value)) + 16
		}

		for _, field := range pt.Fields {
			if len(field.Name) > MaxNameLen {
				return 0, &Error{Code: CodeInvalidArgument, Msg: "field name is too long"}
			}
			value, _ := field.Value.(string)
			if len(value) > MaxStringLen {
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
func (db *DB) Write(table string, points ...Point) error {
	n, err := writeArenaSize(points)
	if err != nil {
		return err
	}

	if len(table) > MaxNameLen {
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
	values := make([][]fieldValue, len(points))

	for p := range points {
		pt := &points[p]

		if pt.Time.Before(minTime) || !pt.Time.Before(maxTime) {
			return &Error{
				Code: CodeInvalidArgument,
				Msg:  fmt.Sprintf("point %d: time must be in [1900-01-01, 2200-01-01) UTC", p),
			}
		}

		values[p] = make([]fieldValue, len(pt.Fields))

		for f := range pt.Fields {
			v, err := convertValue(pt.Fields[f].Value, p, f)
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
			ctags[t].m_name = a.str(tag.Name)
			ctags[t].m_name_length = C.size_t(len(tag.Name))
			ctags[t].m_value = a.str(tag.Value)
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

			cf.m_name = a.str(pt.Fields[f].Name)
			cf.m_name_length = C.size_t(len(pt.Fields[f].Name))
			cf.m_type = v.m_typ

			// The union is raw bytes to Go: an int, an int64, a double, or a
			// pointer and a length.
			u := unsafe.Pointer(&cf.m_value[0])

			switch v.m_typ {
			case C.PULSETEN_TYPE_BOOLEAN:
				*(*C.int)(u) = C.int(v.m_i)
			case C.PULSETEN_TYPE_BIGINT:
				*(*C.int64_t)(u) = C.int64_t(v.m_i)
			case C.PULSETEN_TYPE_DOUBLE:
				*(*C.double)(u) = C.double(v.m_d)
			case C.PULSETEN_TYPE_VARCHAR:
				*(**C.char)(u) = a.str(v.m_s)
				*(*C.size_t)(unsafe.Add(u, unsafe.Sizeof(uintptr(0)))) =
					C.size_t(len(v.m_s))
			}
		}
	}

	h, release, err := db.handle()
	if err != nil {
		return err
	}

	defer release()

	var msg *C.char

	return toError(
		C.pt_go_write(h, ctable, cpoints, C.uint32_t(len(points)), &msg),
		msg)
}
