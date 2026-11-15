// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCreateTable(t *testing.T) {
	opts := DefaultOptions()
	opts.PartitionDurationHours = 24
	opts.FlushInterval = 24 * time.Hour
	db, dir := openTestDB(t, &opts)
	retention := uint32(1)
	create := CreateTableOptions{PartitionDurationHours: 5, RetentionHours: &retention}
	if err := db.CreateTable("CPU", create); err != nil {
		t.Fatal(err)
	}
	tables, err := db.Tables()
	if err != nil || len(tables) != 1 {
		t.Fatalf("%v %v", tables, err)
	}
	initial := tables[0]
	if initial.Name != "cpu" || initial.Initialized || len(initial.Columns) != 0 || initial.PartitionDurationHours != 5 || initial.RetentionHours == nil || *initial.RetentionHours != 1 {
		t.Fatalf("%+v", initial)
	}
	// Metadata is owned by Go, including optional retention values.
	*initial.RetentionHours = 99
	if err := db.CreateTable("cpu", CreateTableOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if rows, err := db.Query("SELECT count(*) FROM cpu"); rows != nil || !errors.Is(err, ErrTableNotInitialized) {
		t.Fatalf("%v %v", rows, err)
	}
	if _, err := db.Explain("SELECT * FROM cpu"); !errors.Is(err, ErrTableNotInitialized) {
		t.Fatal(err)
	}
	if got := queryAll(t, db, "WITH cpu AS (SELECT 9 v) SELECT v FROM cpu"); got[0][0] != int64(9) {
		t.Fatal(got)
	}
	if err := db.Exec("CREATE TABLE cpu(v INT)"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	now := time.Now()
	if err := db.Write("cpu", Point{Time: now.Add(-2 * time.Hour), Fields: []Field{{"v", 1}}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := db.Write("cpu", Point{Time: now, Fields: []Field{{"v", 2}}}); err != nil {
		t.Fatal(err)
	}
	tables, err = db.Tables()
	if err != nil || !tables[0].Initialized || len(tables[0].Columns) != 2 || *tables[0].RetentionHours != 1 {
		t.Fatalf("%v %v", tables, err)
	}
	if initial.Initialized || len(initial.Columns) != 0 {
		t.Fatalf("old snapshot changed: %+v", initial)
	}
	if err := db.CreateTable("pending", CreateTableOptions{}); err != nil {
		t.Fatal(err)
	}
	zero := uint32(0)
	if err := db.SetTableOptions("pending", TableOptions{RetentionHours: &zero}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	opts.PartitionDurationHours = 1
	opts.RetentionHours = 24
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tables, err = db.Tables()
	if err != nil || len(tables) != 2 {
		t.Fatalf("%v %v", tables, err)
	}
	if !tables[0].Initialized || tables[0].PartitionDurationHours != 5 || tables[1].Initialized || tables[1].PartitionDurationHours != 24 || tables[1].RetentionHours == nil || *tables[1].RetentionHours != 0 {
		t.Fatalf("%+v", tables)
	}
	if err := db.CreateTable("inherited", CreateTableOptions{}); err != nil {
		t.Fatal(err)
	}
	tables, err = db.Tables()
	if err != nil || tables[1].Name != "inherited" || tables[1].PartitionDurationHours != 24 || tables[1].RetentionHours != nil {
		t.Fatalf("%v %v", tables, err)
	}
	if err := db.Write("pending", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, "SELECT count(*) FROM pending"); got[0][0] != int64(1) {
		t.Fatal(got)
	}
	db.Close()
	if err := db.CreateTable("closed", CreateTableOptions{}); err == nil {
		t.Fatal("Create after Close succeeded")
	}
}

func TestCreateTableValidationAndRace(t *testing.T) {
	opts := DefaultOptions()
	db, _ := openTestDB(t, &opts)
	tooLong := uint32(2640001)
	window := uint32(5000)
	cases := []struct {
		name     string
		options  CreateTableOptions
		expected error
	}{
		{"", CreateTableOptions{}, ErrInvalidArgument},
		{"_pt_hidden", CreateTableOptions{}, ErrInvalidArgument},
		{"invalid", CreateTableOptions{PartitionDurationHours: 8785}, ErrInvalidArgument},
		{"retention", CreateTableOptions{RetentionHours: &tooLong}, ErrInvalidArgument},
	}
	for _, c := range cases {
		if err := db.CreateTable(c.name, c.options); !errors.Is(err, c.expected) {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	if err := db.CreateTable("wide_window", CreateTableOptions{PartitionDurationHours: 1, RetentionHours: &window}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TABLE ordinary(v INT)"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable("ordinary", CreateTableOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	outcomes := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); outcomes <- db.CreateTable("same", CreateTableOptions{}) }()
	}
	wg.Wait()
	close(outcomes)
	created := 0
	for err := range outcomes {
		if err == nil {
			created++
		} else if !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	if created != 1 {
		t.Fatalf("%d creations", created)
	}
	tables, err := db.Tables()
	if err != nil || len(tables) != 2 || tables[0].Name != "same" || tables[0].Initialized {
		t.Fatalf("%v %v", tables, err)
	}
	if tables[1].Name != "wide_window" || tables[1].PartitionDurationHours != 1 || tables[1].RetentionHours == nil || *tables[1].RetentionHours != window {
		t.Fatalf("%v", tables[1])
	}
}
