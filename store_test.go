package main

import (
	"path/filepath"
	"testing"
)

func TestIDStorePersistsAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "ids.json")
	store, err := OpenIDStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if store.Has("m-1") {
		t.Fatal("unexpected existing ID")
	}
	if err := store.Mark("m-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark("m-1"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenIDStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Has("m-1") {
		t.Fatal("ID was not persisted")
	}
}
