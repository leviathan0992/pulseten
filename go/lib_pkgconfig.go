// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

//go:build pulseten_use_pkgconfig || (!pulseten_use_lib && !(darwin && arm64) && !(linux && amd64) && !(linux && arm64))

package pulseten

// #cgo pkg-config: pulseten
import "C"
