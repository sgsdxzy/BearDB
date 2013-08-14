package beardb

import (
	"encoding/gob"
	"sync"
)

//The compact and most simplified append-and-read-only database
//=============================================================================
type blackBearDB struct {
	storage BearStorage
	rwlock  sync.RWMutex //Appending lock
}

//Non-locking getting size
func (db *blackBearDB) size() int64 {
	return db.storage.Size()
}

//Public methods
//=============================================================================
//Constructor
func NewBlackBearDB(s BearStorage) *blackBearDB {
	return &blackBearDB{storage: s}
}

//Get current size
func (db *blackBearDB) Size() int64 {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	return db.size()
}

//Make sure to close it before exit! Better use defer.
func (db *blackBearDB) Close() error {
	return db.storage.Close()
}

//New Gob Writer. Create one for every thread doing writing
//=============================================================================
type blackBearGobWriter struct {
	w  SafeWriter
	e  *gob.Encoder
	db *blackBearDB
}

func (db *blackBearDB) NewGobWriter() *blackBearGobWriter {
	b := new(blackBearGobWriter)
	b.w = SafeWriter{db.storage, 0}
	b.e = gob.NewEncoder(&b.w) //Must be exactly points to b.w
	b.db = db
	return b
}

//Append item to the end of storage. Id and error(if any) is returned
func (b *blackBearGobWriter) AddItem(item interface{}) (id int64, err error) {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	err = b.e.Encode(item)
	return
}

//Append items to the end of storage. Id of first item and first error(if any)
//encountered is returned
func (b *blackBearGobWriter) AddItems(items ...interface{}) (id int64, err error) {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	for _, item := range items {
		err = b.e.Encode(item)
		if err != nil {
			break
		}
	}
	return
}

//Modify item at id. The serialized size of item must be the same or less.
//It could be very dangerous and is generally discoraged
func (b *blackBearGobWriter) Modify(id int64, item interface{}) error {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	b.w.Offset = id
	return b.e.Encode(item)
}

//Get the underlying DB
func (b *blackBearGobWriter) GetDB() *blackBearDB {
	return b.db
}

//New Gob Reader. Create one for every thread doing reading
//=============================================================================
type blackBearGobReader struct {
	r  SafeReader
	d  *gob.Decoder
	db *blackBearDB
}

func (db *blackBearDB) NewGobReader() *blackBearGobReader {
	b := new(blackBearGobReader)
	b.r = SafeReader{db.storage, 0}
	b.d = gob.NewDecoder(&b.r) //Must be exactly points to b.r
	b.db = db
	return b
}

//Get item at id
func (b *blackBearGobReader) GetItem(id int64, item interface{}) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()
	b.r.Offset = id
	return b.d.Decode(item)
}

//Get items starting from id. If any error occur, the error is returned.
func (b *blackBearGobReader) GetItems(id int64, items ...interface{}) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()
	b.r.Offset = id
	var err error = nil
	for _, item := range items {
		err = b.d.Decode(item)
		if err != nil {
			break
		}
	}
	return err
}

//Get the underlying DB
func (b *blackBearGobReader) GetDB() *blackBearDB {
	return b.db
}

//New Serializer Writer. Create one for every thread doing writing
//=============================================================================
type blackBearSerializerWriter struct {
	w  SafeWriter
	db *blackBearDB
}

func (db *blackBearDB) NewSerializerWriter() *blackBearSerializerWriter {
	b := new(blackBearSerializerWriter)
	b.w = SafeWriter{db.storage, 0}
	b.db = db
	return b
}

//Append item to the end of storage. Id and error(if any) is returned
func (b *blackBearSerializerWriter) AddItem(item Serializer) (id int64, err error) {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	err = item.Serialize(&b.w)

	return
}

//Append items to the end of storage. Id of first item and first error(if any)
//encountered is returned
func (b *blackBearSerializerWriter) AddItems(items ...Serializer) (id int64, err error) {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	for _, item := range items {
		err = item.Serialize(&b.w)
		if err != nil {
			break
		}
	}

	return
}

//Modify item at id. The serialized size of item must be the same or less.
//It could be very dangerous and is generally discoraged
func (b *blackBearSerializerWriter) Modify(id int64, item Serializer) error {
	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	b.w.Offset = id
	return item.Serialize(&b.w)
}

//Get the underlying DB
func (b *blackBearSerializerWriter) GetDB() *blackBearDB {
	return b.db
}

//New Serializer Reader. Create one for every thread doing reading
//=============================================================================
type blackBearSerializerReader struct {
	r  SafeReader
	db *blackBearDB
}

func (db *blackBearDB) NewSerializerReader() *blackBearSerializerReader {
	b := new(blackBearSerializerReader)
	b.r = SafeReader{db.storage, 0}
	b.db = db
	return b
}

//Get item at id
func (b *blackBearSerializerReader) GetItem(id int64, item Serializer) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()

	b.r.Offset = id
	return item.Deserialize(&b.r)
}

//Get items starting from id. If any error occur, the error is returned.
func (b *blackBearSerializerReader) GetItems(id int64, items ...Serializer) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()

	b.r.Offset = id
	var err error = nil
	for _, item := range items {
		err = item.Deserialize(&b.r)
		if err != nil {
			break
		}
	}
	return err
}

//Get the underlying DB
func (b *blackBearSerializerReader) GetDB() *blackBearDB {
	return b.db
}

//=============================================================================
