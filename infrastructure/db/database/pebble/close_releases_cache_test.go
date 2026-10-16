package pebble

import (
	"reflect"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
)

// blockCacheRefs returns the block cache the open pebble instance uses and a reader for the cache's
// reference count. pebble exposes neither, so this reads unexported fields; if pebble renames them the
// test fails here and needs updating.
func blockCacheRefs(t *testing.T, db *DB) func() int64 {
	opts := reflect.ValueOf(db.db).Elem().FieldByName("opts")
	if !opts.IsValid() {
		t.Fatalf("pebble.DB has no opts field; update this test for the pebble version in use")
	}
	cache := (*pebble.Options)(opts.UnsafePointer()).Cache
	refs := reflect.ValueOf(cache).Elem().FieldByName("refs")
	if !refs.IsValid() || refs.Type() != reflect.TypeFor[atomic.Int64]() {
		t.Fatalf("pebble.Cache has no refs counter; update this test for the pebble version in use")
	}
	counter := (*atomic.Int64)(unsafe.Pointer(refs.UnsafeAddr()))
	return counter.Load
}

// TestCloseReleasesTheBlockCache pins that closing the database releases its block cache. The openers
// create the cache, which starts with one reference held by its creator, and pebble.Open adds one for
// the DB. Close only drops the DB's reference, so the cache was never freed.
func TestCloseReleasesTheBlockCache(t *testing.T) {
	openers := map[string]func(string, int) (*DB, error){
		"NewPebbleDB":  NewPebbleDB,
		"OpenPebbleDB": OpenPebbleDB,
	}
	for name, open := range openers {
		t.Run(name, func(t *testing.T) {
			db, err := open(t.TempDir(), 8)
			if err != nil {
				t.Fatalf("%s: %+v", name, err)
			}
			refs := blockCacheRefs(t, db)
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %+v", err)
			}
			if got := refs(); got != 0 {
				t.Fatalf("the block cache has %d references after Close, want 0", got)
			}
		})
	}
}
