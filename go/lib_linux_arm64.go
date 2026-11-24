// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux && arm64 && !pulseten_use_musl && !pulseten_use_pkgconfig && !pulseten_use_lib

package pulseten

// Link the prebuilt libraries for linux/arm64 with glibc.
import _ "github.com/leviathan0992/pulseten-go-libs/linux-arm64"
