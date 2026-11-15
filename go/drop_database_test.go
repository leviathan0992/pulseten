// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDropDatabase(t *testing.T) {
	options := DefaultOptions()
	options.FlushInterval = 24 * time.Hour
	db, dir := openTestDB(t, &options)
	if err := db.Write("m", Point{Time: time.Now(), Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TABLE ordinary(v INT); INSERT INTO ordinary VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	if err := DropDatabase(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("drop open database: %v", err)
	}
	if got := queryAll(t, db, "SELECT count(*) FROM m"); got[0][0] != int64(1) {
		t.Fatal(got)
	}
	db.Close()
	if err := DropDatabase(dir + "\x00ignored"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	extra := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(extra, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := DropDatabase(dir); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if value, err := os.ReadFile(extra); err != nil || string(value) != "preserve" {
		t.Fatalf("%q %v", value, err)
	}
	if err := os.Remove(extra); err != nil {
		t.Fatal(err)
	}
	if err := DropDatabase(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := DropDatabase(dir); err != nil {
		t.Fatal(err)
	}
	var err error
	db, err = Open(dir, &options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if tables, err := db.Tables(); err != nil || len(tables) != 0 {
		t.Fatalf("%v %v", tables, err)
	}
	if rows, err := db.Query("SELECT * FROM ordinary"); err == nil || rows != nil {
		t.Fatalf("%v %v", rows, err)
	}
	db.Close()
	if err := DropDatabase(dir); err != nil {
		t.Fatal(err)
	}
}

func TestDropDatabaseResumeAndLinks(t *testing.T) {
	db, dir := openTestDB(t, nil)
	if err := db.Write("m", Point{Time: time.Now(), Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	alias := filepath.Join(filepath.Dir(dir), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if err := DropDatabase(alias); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if reopened, err := Open(alias, nil); reopened != nil || !errors.Is(err, ErrInvalidArgument) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatalf("open symlink: %v %v", reopened, err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "_pt_.duckdb")); err != nil {
		t.Fatal(err)
	}
	var detail *Error
	if reopened, err := Open(dir, nil); reopened != nil || !errors.As(err, &detail) || detail.Code != CodeCorruption {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatalf("%v %v", reopened, err)
	}
	if err := DropDatabase(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign")
	if err := os.Mkdir(foreign, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(foreign, "_pt_.duckdb")
	if err := os.WriteFile(file, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := DropDatabase(foreign); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if value, err := os.ReadFile(file); err != nil || string(value) != "foreign" {
		t.Fatalf("%q %v", value, err)
	}
	for _, path := range []string{"", "/"} {
		if err := DropDatabase(path); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%q: %v", path, err)
		}
	}
}

// A missing completed store cannot resurrect dropped data from the surviving
// WAL.
func TestMissingStoreAfterDropTable(t *testing.T) {
	options := DefaultOptions()
	options.FlushInterval = 24 * time.Hour
	options.WALSync = true
	db, dir := openTestDB(t, &options)
	if err := db.Write("m", Point{Time: time.Now(), Fields: []Field{{"v", 7}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable("m"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	file := filepath.Join(dir, "_pt_.duckdb")
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(dir, &options)
	if opened != nil {
		opened.Close()
	}
	var detail *Error
	if opened != nil || !errors.As(err, &detail) || detail.Code != CodeCorruption {
		t.Fatalf("missing initialized store: %v %v", opened, err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("Open recreated the missing store: %v", err)
	}
	if err := DropDatabase(dir); err != nil {
		t.Fatal(err)
	}
	fresh, err := Open(dir, &options)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.Write("m", Point{Time: time.Now(), Fields: []Field{{"v", "new"}}}); err != nil {
		t.Fatal(err)
	}
	got := queryAll(t, fresh, "SELECT v FROM m")
	if len(got) != 1 || got[0][0] != "new" {
		t.Fatal(got)
	}
}

// Opening an unmarked nonempty directory cannot create a store beside user
// files.
func TestOpenUnmarkedNonemptyDirectory(t *testing.T) {
	dir := t.TempDir()
	notes := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(notes, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, nil)
	if db != nil {
		db.Close()
	}
	if db != nil || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("%v %v", db, err)
	}
	for _, name := range []string{"_pt_.duckdb", "INITIALIZING", "IDENTITY"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if got, err := os.ReadFile(notes); err != nil || string(got) != "preserve" {
		t.Fatalf("%q %v", got, err)
	}
}
