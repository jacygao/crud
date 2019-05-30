// Package crud provides a simple in memory data storage.
// This package should be used for testing purpose only to implement any CRUD interface as the mock data storage
package crud

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	// ErrKeyExist defines the error value returned when a key already exists on Insert
	ErrKeyExist = errors.New("document key exists")
	// ErrKeyNotExist defines the error value returned when a key doesn't exist on Replace or Remove
	ErrKeyNotExist = errors.New("document key does not exist")
	// ErrCasMismatch defines the error value returned when the Cas provided to Remove doesn't match the actual value
	ErrCasMismatch = errors.New("cas mismatch")
)

// ThirtyDaySeconds seconds in 30 days
const ThirtyDaySeconds = 2592000

// document encapsulates each of the documents with a CAS value
// Regarding TTL:
// - To set a value of 30 days or less : If you want an item to live for less than 30 days, you can provide a TTL in seconds
//   or as Unix time. The maximum value you can specify in seconds is the number of seconds in a month, namely 30 x 24
//   x 60 x 60. Couchbase Server removes the item the given number of seconds after it stores the item.
// - To set a value over 30 days : If you want an item to live for more than 30 days, you must provide a TTL in Unix time.
type document struct {
	Cas uint64
	// TTL of the document
	TTL int64
	// Value contains the raw document data
	Value []byte
}

// getTime is a temporary function that should be replaced with an Exos time
// implementation which allows for tweaking the time for unit tests and
// offsets for QA
func getTime() int64 {
	return time.Now().UTC().Unix()
}

// newDoc is a helper function for creating an initial document state
func newDoc(data []byte, ttl uint32) *document {

	setTTL := int64(ttl)

	// if the ttl value is larger than 0, but less than 30 days,  then assume it's a relative time
	// and calculate it as such
	if setTTL < ThirtyDaySeconds && setTTL > 0 {
		setTTL = getTime() + setTTL
	}
	// else assume that it's a Unix timestamp and set it directly

	return &document{
		Cas:   1,
		Value: data,
		TTL:   setTTL,
	}
}

// Set updates the value and increments the CAS value
func (d *document) set(value []byte) {
	d.Cas++
	d.Value = value
}

// CRUD is a simple object for storing documents
type CRUD struct {
	storage map[string]*document
}

// New creates a crud database for the purposes of mocking a document store
func New() *CRUD {
	return &CRUD{make(map[string]*document)}
}

// Get provides basic Get Database Operation.
// It should be extended and wrapped with application level processes such as validation and serialisation.
func (crud *CRUD) Get(key string, valuePtr interface{}) (uint64, error) {
	doc, ok := crud.storage[key]
	if !ok {
		return 0, ErrKeyNotExist
	}

	// Very basic TTL support
	if doc.TTL > 0 && doc.TTL < getTime() {
		delete(crud.storage, key)
		return 0, ErrKeyNotExist
	}

	if err := json.Unmarshal(doc.Value, valuePtr); err != nil {
		return 0, err
	}

	return doc.Cas, nil
}

// Insert provides basic Insert Database Operation. It should be extended and wrapped with application level processes such as validation and serialisation.
func (crud *CRUD) Insert(key string, value interface{}, expiry uint32) (uint64, error) {
	if doc, ok := crud.storage[key]; ok {
		return doc.Cas, ErrKeyExist
	}

	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}

	doc := newDoc(data, expiry)
	crud.storage[key] = doc

	return doc.Cas, nil
}

// Upsert provides basic Upsert Database Operation. It should be extended and wrapped with application level processes such as validation and serialisation.
// Upsert will also attempt to flush cache of the key if the database operation is successful.
func (crud *CRUD) Upsert(key string, value interface{}, expiry uint32) (uint64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}

	if doc, ok := crud.storage[key]; ok {
		doc.set(data)
		return doc.Cas, nil
	}

	doc := newDoc(data, expiry)
	crud.storage[key] = doc
	return doc.Cas, nil
}

// Replace provides basic Replace Database Operation. It should be extended and wrapped with application level processes such as validation and serialisation.
// Replace will also attempt to flush cache of the key if the database operation is successful.
func (crud *CRUD) Replace(key string, value interface{}, cas uint64, expiry uint32) (uint64, error) {
	doc, ok := crud.storage[key]
	if !ok {
		return doc.Cas, ErrKeyNotExist
	}

	// Very basic TTL support
	if doc.TTL > 0 && doc.TTL < getTime() {
		delete(crud.storage, key)
		return 0, ErrKeyNotExist
	}

	// Check that the Cas on the request is accurate
	if doc.Cas != cas {
		return 0, ErrCasMismatch
	}

	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}

	doc = newDoc(data, expiry)
	// Manually insert the CAS value also tracking this op
	cas++
	doc.Cas = cas
	crud.storage[key] = doc

	return doc.Cas, nil
}

// Remove provides basic Remove Database Operation. It should be extended and wrapped with application level processes such as validation and serialisation.
// Remove will also attempt to flush cache of the key if the database operation is successful.
func (crud *CRUD) Remove(key string, cas uint64) (uint64, error) {
	if _, exists := crud.storage[key]; !exists {
		return 0, ErrKeyNotExist
	}

	if crud.storage[key].Cas == cas {
		// skip expired data check here and just delete it all the same
		delete(crud.storage, key)

		return cas, nil
	}

	return 0, ErrCasMismatch
}

// Touch updates the document expiry time.  Chaning the expiry time will also change the document's CAS value
func (crud *CRUD) Touch(key string, cas uint64, expiry uint32) (uint64, error) {
	doc, exists := crud.storage[key]
	if !exists {
		return doc.Cas, ErrKeyNotExist
	}

	// Check that the Cas on the request is accurate
	if doc.Cas != cas {
		return 0, ErrCasMismatch
	}

	// Update the expiry
	newTTL := int64(expiry)

	// if the ttl value is larger than 0, but less than 30 days,  then assume it's a relative time
	// and calculate it as such
	if newTTL < ThirtyDaySeconds && newTTL > 0 {
		newTTL = getTime() + newTTL
	}
	// else assume that it's a Unix timestamp and set it directly
	doc.TTL = newTTL

	// FIXME: Should the CAS value be incremented for this op?
	doc.Cas++

	// Update the document in the 'db'
	crud.storage[key] = doc

	return doc.Cas, nil
}

func (crud *CRUD) IsKeyNotFoundError(err error) bool {
	return err == ErrKeyNotExist
}
