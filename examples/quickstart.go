// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

// Write a point and query it back.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	pulseten "github.com/leviathan0992/pulseten/go"
)

func main() {
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

	if err := db.Write("cpu", pulseten.Point{
		Time:   time.Now(),
		Tags:   []pulseten.Tag{{Name: "host", Value: "web-1"}},
		Fields: []pulseten.Field{{Name: "usage", Value: 12.5}},
	}); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT host, avg(usage) FROM cpu GROUP BY host")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		fmt.Println(rows.String(0), rows.Float64(1))
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("pulseten", pulseten.Version())
}
