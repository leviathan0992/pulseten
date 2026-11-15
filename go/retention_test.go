// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"errors"
	"testing"
	"time"
)

func TestTableRetention(t *testing.T) {
	opts := DefaultOptions()
	opts.FlushInterval = 24 * time.Hour
	db, dir := openTestDB(t, &opts)
	now := time.Now()
	old := Point{Time: now.Add(-2 * time.Hour), Fields: []Field{{"v", 10}}}
	recent := Point{Time: now, Fields: []Field{{"v", 1}}}
	if err := db.Write("cpu", old, recent); err != nil {
		t.Fatal(err)
	}
	hour := uint32(1)
	if err := db.SetTableOptions("CPU", TableOptions{RetentionHours: &hour}); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, "SELECT count(*), sum(v) FROM cpu"); got[0][0] != int64(1) || got[0][1] != int64(1) {
		t.Fatal(got)
	}
	if err := db.Write("cpu", old); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := db.SetTableOptions("missing", TableOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	for _, invalid := range []uint32{2640001, ^uint32(0)} {
		if err := db.SetTableOptions("cpu", TableOptions{RetentionHours: &invalid}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("SetTableOptions retention %d: %v", invalid, err)
		}
	}
	db.Close()
	opts.RetentionHours = 24
	var err error
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := queryAll(t, db, "SELECT count(*) FROM cpu"); got[0][0] != int64(1) {
		t.Fatal(got)
	}
	if err := db.SetTableOptions("cpu", TableOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, "SELECT count(*) FROM cpu"); got[0][0] != int64(2) {
		t.Fatal(got)
	}
	zero := uint32(0)
	if err := db.SetTableOptions("cpu", TableOptions{RetentionHours: &zero}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	opts.RetentionHours = 1
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := queryAll(t, db, "SELECT count(*) FROM cpu"); got[0][0] != int64(2) {
		t.Fatal(got)
	}
	db.Close()
	if err := db.SetTableOptions("cpu", TableOptions{}); err == nil {
		t.Fatal("Set after Close succeeded")
	}
}

func TestPartitionGrowth(t *testing.T) {
	opts := DefaultOptions()
	opts.PartitionDurationHours = 1
	db, _ := openTestDB(t, &opts)
	hour := time.Now().Truncate(time.Hour)
	p := Point{Time: hour, Fields: []Field{{"v", 1}}}
	if err := db.Write("m", p); err != nil {
		t.Fatal(err)
	}
	p.Time = hour.Add(time.Hour)
	if err := db.Write("m", p); err != nil {
		t.Fatal(err)
	}
	p.Time = hour.Add(2 * time.Hour)
	if err := db.Write("m", p); err != nil {
		t.Fatal(err)
	}
	retention := uint32(5000)
	if err := db.SetTableOptions("m", TableOptions{RetentionHours: &retention}); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, "SELECT count(*) FROM m"); got[0][0] != int64(3) {
		t.Fatal(got)
	}
}

// Future limits are opt-in, apply to complete batches and never block replay.
func TestMaxFutureSkew(t *testing.T) {
	options := DefaultOptions()
	if options.MaxFutureSkew != 0 {
		t.Fatal(options.MaxFutureSkew)
	}
	options.FlushInterval = 24 * time.Hour
	db, dir := openTestDB(t, &options)
	p := Point{Time: time.Now().Add(24 * time.Hour), Fields: []Field{{"v", 1}}}
	if err := db.Write("m", p); err != nil {
		t.Fatal(err)
	}
	db.Close()
	options.MaxFutureSkew = 5 * time.Minute
	db, err := Open(dir, &options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := queryAll(t, db, "SELECT count(*) FROM m"); got[0][0] != int64(1) {
		t.Fatal(got)
	}
	before := db.Stats().VisibleSeq
	if err := db.Write("m", Point{Time: time.Now(), Fields: []Field{{"extra", 2}}}, p); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if db.Stats().VisibleSeq != before {
		t.Fatal("rejected batch advanced sequence")
	}
	tables, err := db.Tables()
	if err != nil || len(tables) != 1 || len(tables[0].Columns) != 2 {
		t.Fatalf("%v %v", tables, err)
	}
	if err := db.Write("m", Point{Time: time.Now().Add(time.Minute), Fields: []Field{{"v", 2}}}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	for _, skew := range []time.Duration{-time.Second, time.Nanosecond, (1 << 32) * time.Second} {
		options.MaxFutureSkew = skew
		opened, err := Open(dir, &options)
		if opened != nil {
			opened.Close()
		}
		if opened != nil || !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%v: %v", skew, err)
		}
	}
}
