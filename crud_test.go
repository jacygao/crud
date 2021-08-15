package crud

import (
	"reflect"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	client := New()
	cas, err := client.Insert("key", "val", 1)
	if err != nil {
		t.Fatal(err)
	}
	if cas != 1 {
		t.Fatal("cas mismatch")
	}
}

func TestGet(t *testing.T) {
	client := New()
	cas, _ := client.Insert("key", "val", 1)

	var act string
	cas2, err := client.Get("key", &act)
	if err != nil {
		t.Fatal(err)
	}

	if cas != cas2 {
		t.Fatal("cas mismatch")
	}

	if act != "val" {
		t.Fatal("results mismatch")
	}
}

func TestUpsert(t *testing.T) {
	client := New()
	cas, _ := client.Insert("key", "val", 1)

	cas2, err := client.Upsert("key", "val2", 1)
	if err != nil {
		t.Fatal(err)
	}

	if cas2 != cas+1 {
		t.Fatal("cas mismatch")
	}

	var act string
	cas3, _ := client.Get("key", &act)

	if cas3 != cas2 {
		t.Fatal("cas mismatch")
	}

	if act != "val2" {
		t.Fatal("results mismatch")
	}
}

func TestReplace(t *testing.T) {
	client := New()
	cas, _ := client.Insert("key", "val", 1)

	cas2, err := client.Replace("key", "val2", cas, 1)
	if err != nil {
		t.Fatal(err)
	}

	if cas2 != cas+1 {
		t.Fatal("cas mismatch")
	}

	var act string
	cas3, _ := client.Get("key", &act)

	if cas3 != cas2 {
		t.Fatal("cas mismatch")
	}

	if act != "val2" {
		t.Fatal("results mismatch")
	}
}

func TestReplaceCasMismatch(t *testing.T) {
	client := New()
	_, _ = client.Insert("key", "val", 1)

	_, err := client.Replace("key", "val2", 2, 1)
	if !reflect.DeepEqual(err, ErrCasMismatch) {
		t.Fatal("error mismatch")
	}
}

func TestRemove(t *testing.T) {
	client := New()
	cas, _ := client.Insert("key", "val", 1)

	cas2, err := client.Remove("key", cas)
	if err != nil {
		t.Fatal(err)
	}

	if cas2 != cas {
		t.Fatal("cas mismatch")
	}

	var act string
	_, err = client.Get("key", &act)
	if !reflect.DeepEqual(err, ErrKeyNotExist) {
		t.Fatal("error mismatch")
	}
}

func TestTouch(t *testing.T) {
	client := New()
	cas, _ := client.Insert("key", "val", 0)

	cas2, err := client.Touch("key", cas, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cas2 != cas+1 {
		t.Fatal("cas mismatch")
	}

	time.Sleep(time.Second * 2)

	var act string
	_, err = client.Get("key", &act)
	if !reflect.DeepEqual(err, ErrKeyNotExist) {
		t.Fatal("error mismatch")
	}
}
