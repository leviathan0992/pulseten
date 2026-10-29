// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

/*
#include <stdlib.h>
#include <pulseten.h>
*/
import "C"

import "unsafe"

// Code is the status of a failed call, as pulseten_state_t.
type Code int

// The status codes; see pulseten.h for what each one means.
const (
	// CodeError is a SQL error, or a failure with no more specific code.
	CodeError Code = C.PULSETEN_ERROR

	// CodeInvalidArgument means a write or statement was rejected; nothing
	// changed.
	CodeInvalidArgument Code = C.PULSETEN_INVALID_ARGUMENT

	// CodeSchemaConflict means a write gives an existing column another role
	// or type.
	CodeSchemaConflict Code = C.PULSETEN_SCHEMA_CONFLICT

	// CodeLimitExceeded means a size or count limit was exceeded.
	CodeLimitExceeded Code = C.PULSETEN_LIMIT_EXCEEDED

	// CodeOutOfMemory means a memory allocation failed.
	CodeOutOfMemory Code = C.PULSETEN_OUT_OF_MEMORY

	// CodeIOError means an operating system I/O call failed.
	CodeIOError Code = C.PULSETEN_IO_ERROR

	// CodeCorruption means stored data failed validation.
	CodeCorruption Code = C.PULSETEN_CORRUPTION

	// CodeLocked means another handle or process holds the directory lock.
	CodeLocked Code = C.PULSETEN_LOCKED

	// CodeFenced means an earlier failure stopped writes and flushes; close
	// and reopen.
	CodeFenced Code = C.PULSETEN_FENCED

	// CodeOutcomeUnknown means a write or flush may or may not be durable;
	// do not retry blindly. Close and reopen to find out.
	CodeOutcomeUnknown Code = C.PULSETEN_OUTCOME_UNKNOWN

	// CodeInterrupted means Rows.Interrupt stopped the query.
	CodeInterrupted Code = C.PULSETEN_INTERRUPTED

	// CodeNotFound means the named table does not exist.
	CodeNotFound Code = C.PULSETEN_NOT_FOUND
)

// String names the code, e.g. "INVALID_ARGUMENT".
func (c Code) String() string {
	return C.GoString(C.pulseten_state_name(C.pulseten_state_t(c)))
}

// Error is a failed call: its code and PulseTen's message.
type Error struct {
	// Code is the status of the call.
	Code Code

	// Msg is the message, possibly empty.
	Msg string
}

// Error formats the code and the message.
func (e *Error) Error() string {
	if e.Msg == "" {
		return "pulseten: " + e.Code.String()
	}

	return "pulseten: " + e.Code.String() + ": " + e.Msg
}

// Is matches another *Error with the same code, so that
// errors.Is(err, pulseten.ErrNotFound) works whatever the message.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)

	return ok && t.Code == e.Code
}

// Errors to compare with errors.Is; only their codes matter.
var (
	// ErrInvalidArgument matches errors with CodeInvalidArgument.
	ErrInvalidArgument = &Error{Code: CodeInvalidArgument}

	// ErrSchemaConflict matches errors with CodeSchemaConflict.
	ErrSchemaConflict = &Error{Code: CodeSchemaConflict}

	// ErrLimitExceeded matches errors with CodeLimitExceeded.
	ErrLimitExceeded = &Error{Code: CodeLimitExceeded}

	// ErrLocked matches errors with CodeLocked.
	ErrLocked = &Error{Code: CodeLocked}

	// ErrFenced matches errors with CodeFenced.
	ErrFenced = &Error{Code: CodeFenced}

	// ErrOutcomeUnknown matches errors with CodeOutcomeUnknown.
	ErrOutcomeUnknown = &Error{Code: CodeOutcomeUnknown}

	// ErrInterrupted matches errors with CodeInterrupted.
	ErrInterrupted = &Error{Code: CodeInterrupted}

	// ErrNotFound matches errors with CodeNotFound.
	ErrNotFound = &Error{Code: CodeNotFound}
)

// toError turns a status and the message copied by the shim into an error,
// and frees the message.
func toError(state C.pulseten_state_t, msg *C.char) error {
	if state == C.PULSETEN_OK {
		return nil
	}

	e := &Error{Code: Code(state)}

	if msg != nil {
		e.Msg = C.GoString(msg)
		C.free(unsafe.Pointer(msg))
	}

	return e
}
