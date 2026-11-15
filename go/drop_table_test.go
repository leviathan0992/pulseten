// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDropTable(t *testing.T) {
	opts := DefaultOptions()
	opts.FlushInterval = 24 * time.Hour
	opts.PartitionDurationHours = 24
	db, dir := openTestDB(t, &opts)
	zero := uint32(0)
	if err := db.CreateTable("cpu", CreateTableOptions{PartitionDurationHours: 1, RetentionHours: &zero}); err != nil {
		t.Fatal(err)
	}
	p := Point{Time: time.Now(), Fields: []Field{{"v", 1}}}
	if err := db.Write("cpu", p); err != nil {
		t.Fatal(err)
	}
	if err := db.Write("other", p); err != nil {
		t.Fatal(err)
	}
	before, err := db.Tables()
	if err != nil {
		t.Fatal(err)
	}
	other, err := db.Query("SELECT v FROM other") // Pins batches containing old cpu rows too.
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := db.DropTable("CPU"); err != nil {
		t.Fatal(err)
	}
	if db.Stats().FlushedSeq != 2 {
		t.Fatalf("drop did not flush visible prefix: %+v", db.Stats())
	}
	if rows, err := db.Query("SELECT * FROM cpu"); rows != nil || err == nil || errors.Is(err, ErrTableNotInitialized) {
		t.Fatalf("%v %v", rows, err)
	}
	if err := db.DropTable("cpu"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := db.SetTableOptions("cpu", TableOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := db.CreateTable("cpu", CreateTableOptions{PartitionDurationHours: 5}); err != nil {
		t.Fatal(err)
	}
	if err := db.Write("cpu", Point{Time: p.Time, Fields: []Field{{"v", "new"}}}); err != nil {
		t.Fatal(err)
	}
	got := queryAll(t, db, "SELECT v FROM cpu")
	if len(got) != 1 || got[0][0] != "new" {
		t.Fatal(got)
	}
	tables, err := db.Tables()
	if err != nil || len(tables) != 2 || tables[0].PartitionDurationHours != 5 || tables[0].RetentionHours != nil || !tables[0].Initialized {
		t.Fatalf("%v %v", tables, err)
	}
	if before[0].PartitionDurationHours != 1 || before[0].Columns[1].Type != TypeBigint {
		t.Fatalf("old snapshot changed: %v", before)
	}
	if !other.Next() || other.Value(0) != int64(1) {
		t.Fatalf("old reader: %v", other.Err())
	}
	other.Close()
	db.Close()
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := queryAll(t, db, "SELECT v FROM cpu"); len(got) != 1 || got[0][0] != "new" {
		t.Fatal(got)
	}
	if err := db.DropTable("cpu"); err != nil {
		t.Fatal(err)
	}
	if err := db.Write("cpu", p); err != nil {
		t.Fatal(err)
	} // Automatic recreation uses the database defaults.
	tables, err = db.Tables()
	if err != nil || tables[0].PartitionDurationHours != 24 || tables[0].RetentionHours != nil {
		t.Fatalf("%v %v", tables, err)
	}
	if got := queryAll(t, db, "SELECT v FROM cpu"); len(got) != 1 || got[0][0] != int64(1) {
		t.Fatal(got)
	}
	db.Close()
	if err := db.DropTable("cpu"); err == nil {
		t.Fatal("Drop after Close succeeded")
	}
}

func TestDropTableLocksAndValidation(t *testing.T) {
	opts := DefaultOptions()
	opts.FlushInterval = 24 * time.Hour
	opts.WriteLockTimeout = 20 * time.Millisecond
	db, _ := openTestDB(t, &opts)
	for _, name := range []string{"", "_pt_hidden", "missing"} {
		if err := db.DropTable(name); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	if err := db.Exec("CREATE TABLE ordinary(v INT)"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable("ordinary"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := db.CreateTable("empty", CreateTableOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable("empty"); err != nil {
		t.Fatal(err)
	}
	if err := db.Write("m", Point{Time: time.Now(), Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT * FROM m")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable("m"); !errors.Is(err, ErrLockTimeout) {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("reader lost rows: %v", rows.Err())
	}
	rows.Close()
	if err := db.DropTable("m"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable("m", CreateTableOptions{}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	outcomes := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); outcomes <- db.DropTable("m") }()
	}
	wg.Wait()
	close(outcomes)
	dropped := 0
	for err := range outcomes {
		if err == nil {
			dropped++
		} else if !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	if dropped != 1 {
		t.Fatalf("%d successful drops", dropped)
	}
	tables, err := db.Tables()
	if err != nil || len(tables) != 0 {
		t.Fatalf("%v %v", tables, err)
	}
}
