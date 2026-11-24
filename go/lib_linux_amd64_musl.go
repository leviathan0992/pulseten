// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux && amd64 && pulseten_use_musl && !pulseten_use_pkgconfig && !pulseten_use_lib

package pulseten

// Link the prebuilt libraries for linux/amd64 with musl.
import _ "github.com/leviathan0992/pulseten-go-libs/linux-amd64-musl"
