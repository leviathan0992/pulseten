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

// Tag is a VARCHAR column with the tag role.
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
	// typ is the column type.
	typ C.pulseten_type_t

	// i holds a BOOLEAN as 0 or 1, or a BIGINT.
	i int64

	// d holds a DOUBLE.
	d float64

	// s holds a VARCHAR.
	s string
}

// convertValue converts a field value; p and f locate it for the error.
func convertValue(v any, p, f int) (fieldValue, error) {
	switch x := v.(type) {
	case bool:
		if x {
			return fieldValue{typ: C.PULSETEN_TYPE_BOOLEAN, i: 1}, nil
		}

		return fieldValue{typ: C.PULSETEN_TYPE_BOOLEAN}, nil
	case int:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case int8:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case int16:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case int32:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case int64:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: x}, nil
	case uint:
		if uint64(x) > 1<<63-1 {
			break
		}

		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case uint8:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case uint16:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case uint32:
		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case uint64:
		if x > 1<<63-1 {
			break
		}

		return fieldValue{typ: C.PULSETEN_TYPE_BIGINT, i: int64(x)}, nil
	case float32:
		return fieldValue{typ: C.PULSETEN_TYPE_DOUBLE, d: float64(x)}, nil
	case float64:
		return fieldValue{typ: C.PULSETEN_TYPE_DOUBLE, d: x}, nil
	case string:
		return fieldValue{typ: C.PULSETEN_TYPE_VARCHAR, s: x}, nil
	}

	return fieldValue{}, &Error{
		Code: CodeInvalidArgument,
		Msg:  fmt.Sprintf("point %d field %d: unsupported value of type %T", p, f, v),
	}
}

// arena is one C allocation holding a whole batch, since cgo does not let C
// keep Go pointers inside C structs.
type arena struct {
	// base is the allocation.
	base unsafe.Pointer

	// used is the bytes handed out so far.
	used uintptr
}

// alloc hands out n bytes aligned for any C type.
func (a *arena) alloc(n uintptr) unsafe.Pointer {
	a.used = (a.used + 7) &^ 7
	p := unsafe.Add(a.base, a.used)
	a.used += n

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

// Write writes a batch of points to a table, creating the table and any new
// columns, and returns the write's sequence number. The batch is accepted or
// rejected as a whole; once Write returns a nil error it is durable and
// visible to new queries.
func (db *DB) Write(table string, points ...Point) (uint64, error) {
	// The C counts are 32-bit; larger ones would wrap around before the
	// library could reject them.
	if len(points) > MaxPoints {
		return 0, &Error{
			Code: CodeLimitExceeded,
			Msg:  fmt.Sprintf("batch has more than %d points", MaxPoints),
		}
	}

	ctable, err := checkedCString(table, "table name")
	if err != nil {
		return 0, err
	}
	defer C.free(unsafe.Pointer(ctable))

	// Check the original instant before UnixMicro can overflow.
	minTime := time.UnixMicro(int64(C.PULSETEN_MIN_TIME_US))
	maxTime := time.UnixMicro(int64(C.PULSETEN_MAX_TIME_US))

	// Convert the values and size the arena.
	n := uintptr(len(points)) * unsafe.Sizeof(C.pulseten_point_t{})
	values := make([][]fieldValue, len(points))

	for p := range points {
		pt := &points[p]

		if pt.Time.Before(minTime) || !pt.Time.Before(maxTime) {
			return 0, &Error{
				Code: CodeInvalidArgument,
				Msg:  fmt.Sprintf("point %d: time must be in [1900-01-01, 2200-01-01) UTC", p),
			}
		}

		if len(pt.Tags)+len(pt.Fields) >= MaxColumns {
			return 0, &Error{
				Code: CodeLimitExceeded,
				Msg: fmt.Sprintf("point %d: more than %d tags and fields", p,
					MaxColumns-1),
			}
		}

		n += uintptr(len(pt.Tags))*unsafe.Sizeof(C.pulseten_tag_t{}) + 8
		n += uintptr(len(pt.Fields))*unsafe.Sizeof(C.pulseten_field_t{}) + 8

		for _, t := range pt.Tags {
			n += uintptr(len(t.Name)+len(t.Value)) + 16
		}

		values[p] = make([]fieldValue, len(pt.Fields))

		for f := range pt.Fields {
			v, err := convertValue(pt.Fields[f].Value, p, f)
			if err != nil {
				return 0, err
			}

			values[p][f] = v
			n += uintptr(len(pt.Fields[f].Name)+len(v.s)) + 16
		}
	}

	a := arena{base: C.malloc(C.size_t(n + 8))}
	if a.base == nil {
		return 0, &Error{Code: CodeOutOfMemory}
	}
	defer C.free(a.base)

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

		cp.time_us = C.int64_t(pt.Time.UnixMicro())
		cp.n_tags = C.uint32_t(len(pt.Tags))
		cp.n_fields = C.uint32_t(len(pt.Fields))

		if len(pt.Tags) > 0 {
			cp.tags = (*C.pulseten_tag_t)(
				a.alloc(uintptr(len(pt.Tags)) * unsafe.Sizeof(C.pulseten_tag_t{})))
		}

		ctags := unsafe.Slice(cp.tags, len(pt.Tags))

		for t, tag := range pt.Tags {
			ctags[t].name = a.str(tag.Name)
			ctags[t].name_len = C.size_t(len(tag.Name))
			ctags[t].value = a.str(tag.Value)
			ctags[t].value_len = C.size_t(len(tag.Value))
		}

		if len(pt.Fields) > 0 {
			cp.fields = (*C.pulseten_field_t)(
				a.alloc(uintptr(len(pt.Fields)) * unsafe.Sizeof(C.pulseten_field_t{})))
		}

		cfields := unsafe.Slice(cp.fields, len(pt.Fields))

		for f := range pt.Fields {
			cf := &cfields[f]
			v := &values[p][f]

			cf.name = a.str(pt.Fields[f].Name)
			cf.name_len = C.size_t(len(pt.Fields[f].Name))
			cf._type = v.typ

			// The union is raw bytes to Go: an int, an int64, a double, or a
			// pointer and a length.
			u := unsafe.Pointer(&cf.value[0])

			switch v.typ {
			case C.PULSETEN_TYPE_BOOLEAN:
				*(*C.int)(u) = C.int(v.i)
			case C.PULSETEN_TYPE_BIGINT:
				*(*C.int64_t)(u) = C.int64_t(v.i)
			case C.PULSETEN_TYPE_DOUBLE:
				*(*C.double)(u) = C.double(v.d)
			case C.PULSETEN_TYPE_VARCHAR:
				*(**C.char)(u) = a.str(v.s)
				*(*C.size_t)(unsafe.Add(u, unsafe.Sizeof(uintptr(0)))) =
					C.size_t(len(v.s))
			}
		}
	}

	h, release, err := db.handle()
	if err != nil {
		return 0, err
	}
	defer release()

	var seq C.uint64_t
	var msg *C.char

	err = toError(
		C.pt_go_write(h, ctable, cpoints, C.uint32_t(len(points)), &seq, &msg),
		msg)

	return uint64(seq), err
}
