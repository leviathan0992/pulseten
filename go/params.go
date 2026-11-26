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
	"math"
	"time"
	"unsafe"
)

// minParamTime is the earliest time DuckDB can convert; maxParamTime stands for
// infinity.
var (
	minParamTime = time.UnixMicro(-9223372022400000000)
	maxParamTime = time.UnixMicro(math.MaxInt64)
)

// convertParam converts the argument at index i of a statement.
func convertParam(v any, i int) (inputValue, error) {
	switch x := v.(type) {
	case nil:
		return inputValue{m_is_null: true}, nil

	case time.Time:
		// Check the original instant before UnixMicro can overflow.
		if x.Before(minParamTime) || !x.Before(maxParamTime) {
			return inputValue{}, &Error{
				Code: CodeInvalidArgument,
				Msg:  fmt.Sprintf("parameter $%d: time is out of range", i+1),
			}
		}

		return inputValue{m_type: C.PULSETEN_TYPE_TIMESTAMP, m_integer: x.UnixMicro()}, nil

	case string:
		if len(x) > MaxStringBytes {
			return inputValue{}, &Error{
				Code: CodeLimitExceeded,
				Msg:  fmt.Sprintf("parameter $%d: string is too long", i+1),
			}
		}
	}

	value, ok := convertValue(v)
	if !ok {
		return inputValue{}, &Error{
			Code: CodeInvalidArgument,
			Msg:  fmt.Sprintf("parameter $%d: unsupported value of type %T", i+1, v),
		}
	}

	return value, nil
}

// params are the arguments of a statement, converted to C.
type params struct {
	// m_base is the C allocation; nil without arguments.
	m_base unsafe.Pointer

	// m_first is the first C parameter; nil without arguments.
	m_first *C.pulseten_param_t

	// m_count is the number of parameters.
	m_count C.uint32_t
}

// newParams converts the arguments of a statement. The caller frees them.
func newParams(args []any) (params, error) {
	if len(args) == 0 {
		return params{}, nil
	}

	if uint64(len(args)) > math.MaxUint32 {
		return params{}, &Error{Code: CodeLimitExceeded, Msg: "too many parameters"}
	}

	arrayBytes := uint64(len(args)) * uint64(unsafe.Sizeof(C.pulseten_param_t{}))
	arenaBytes := arrayBytes + 8
	values := make([]inputValue, len(args))

	for i, arg := range args {
		value, err := convertParam(arg, i)
		if err != nil {
			return params{}, err
		}

		values[i] = value
		arenaBytes += uint64(len(value.m_string)) + 8
	}

	if arenaBytes > uint64(^uintptr(0)) {
		return params{}, &Error{Code: CodeLimitExceeded, Msg: "C input allocation is too large"}
	}

	a := arena{m_base: C.pt_go_malloc(C.size_t(arenaBytes))}
	if a.m_base == nil {
		return params{}, &Error{Code: CodeOutOfMemory}
	}

	first := (*C.pulseten_param_t)(a.alloc(uintptr(arrayBytes)))
	cparams := unsafe.Slice(first, len(args))

	for i := range values {
		cparams[i] = C.pulseten_param_t{}

		if values[i].m_is_null {
			cparams[i].m_is_null = 1

			continue
		}

		cparams[i].m_type = values[i].m_type
		a.storeValue(unsafe.Pointer(&cparams[i].m_value[0]), &values[i])
	}

	return params{m_base: a.m_base, m_first: first, m_count: C.uint32_t(len(args))}, nil
}

// free releases the C allocation.
func (p params) free() {
	C.free(p.m_base)
}
