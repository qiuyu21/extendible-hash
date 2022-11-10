package extendible_hash_table

import (
	"fmt"
	"hash/fnv"
	"testing"
)

func hash(s *string) int {
	h := fnv.New32()
	h.Write([]byte(*s))
	return int(h.Sum32())
}

func TestInsert(t *testing.T) {
	m := New[string, int](3, hash)
	var keys []string
	var vals []int

	n := 24;

	for i := 0; i < n; i++ { 
		key := fmt.Sprintf("%v", string(byte(97+i)))
		val := i
		keys = append(keys, key)
		vals = append(vals, val)
		m.Insert(&key, &val)
	}

	if m.GetNumberOfKeys() != len(keys) {
		t.Fatalf("Inserted %v keys, but found %v keys", len(keys), m.GetNumberOfKeys())
	}

	for i, key := range keys {
		var val int
		if !m.Find(&key, &val) { 
			t.Fatalf("key: %v not found", key) 
		} else if vals[i] != val { 
			t.Fatalf("key: %v has value %v, but found %v.", key, vals[i], val)
		}
	}
}

func TestRemove(t *testing.T) {
	m := New[string, int](3, hash)
	var keys []string

	n := 24;

	for i := 0; i < n; i++ { 
		key := fmt.Sprintf("%v", string(byte(97+i)))
		val := i
		keys = append(keys, key)
		m.Insert(&key, &val)
	}

	for i, key := range keys {
		if i % 2 == 0 { continue }
		if !m.Remove(&key) {
			t.Fatalf("key: %v not found.", key)
		}
	}

	if m.GetNumberOfKeys() != n/2 {
		t.Fatalf("The number of keys left not equal to %v.", n/2)
	}

	key := "abc"
	if m.Remove(&key) {
		t.Fatalf("key: %v should not be found.", key)
	}
}

func TestUpdate(t *testing.T) {
	m := New[string, int](3, hash)
	
	var keys []string
	var vals []int

	n := 24;

	for i := 0; i < n; i++ { 
		key := fmt.Sprintf("%v", string(byte(97+i)))
		val := i
		keys = append(keys, key)
		vals = append(vals, val)
		m.Insert(&key, &val)
	}

	if m.GetNumberOfKeys() != len(keys) {
		t.Fatalf("Inserted %v keys, but found %v keys", len(keys), m.GetNumberOfKeys())
	}

	newvals := make([]int, len(vals))
	for i, key := range keys {
		newvals[i] = vals[i] + 100
		m.Insert(&key, &newvals[i])
	}

	if m.GetNumberOfKeys() != len(keys) {
		t.Fatalf("Updated all %v keys and added no new key, but found %v keys", len(keys), m.GetNumberOfKeys())
	}

	for i, key := range keys {
		var val int
		if !m.Find(&key, &val) { 
			t.Fatalf("key: %v not found", key) 
		} else if val != newvals[i] { 
			t.Fatalf("key: %v has value %v, but found %v.", key, newvals[i], val)
		}
	}
}