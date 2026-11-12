// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	pulseten "github.com/pulseten/pulseten/go"
)

// Example writes two points and queries them.
func Example() {
	dir, err := os.MkdirTemp("", "pulseten")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir)

	db, err := pulseten.Open(filepath.Join(dir, "db"), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	err = db.Write("cpu",
		pulseten.Point{
			Time:   now,
			Tags:   []pulseten.Tag{{Name: "host", Value: "web-1"}},
			Fields: []pulseten.Field{{Name: "usage", Value: 12.5}},
		},
		pulseten.Point{
			Time:   now,
			Tags:   []pulseten.Tag{{Name: "host", Value: "web-2"}},
			Fields: []pulseten.Field{{Name: "usage", Value: 30.0}},
		})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT host, usage FROM cpu WHERE usage > 20")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		fmt.Printf("%s %.2f\n", rows.String(0), rows.Float64(1))
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Output: web-2 30.00
}
