package beardb

import (
	"encoding/gob"
)

//The compact and most simplified append-and-read-only database
//=============================================================================
type blackBearDB struct {
	storage   BearStorage
	appending chan bool //Appending lock
}

func NewBlackBearDB(s BearStorage) *blackBearDB {
	return &blackBearDB{s, make(chan bool, 1)}
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
	b.db.appending <- true //Lock the db for appending

	id = b.db.Size()
	b.w.Offset = id
	err = b.e.Encode(item)

	<-b.db.appending //Unlock
	return
}

//Append items to the end of storage. Id of first item and first error(if any)
//encountered is returned
func (b *blackBearGobWriter) AddItems(items ...interface{}) (id int64, err error) {
	b.db.appending <- true //Lock the db for appending

	id = b.db.Size()
	b.w.Offset = id
	for _, item := range items {
		err = b.e.Encode(item)
		if err != nil {
			break
		}
	}

	<-b.db.appending //Unlock
	return
}

//Modify item at id. The serialized size of item must be the same or less.
//It could be very dangerous and is generally discoraged
func (b *blackBearGobWriter) Modify(id int64, item interface{}) error {
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
	b.r.Offset = id
	return b.d.Decode(item)
}

//Get items starting from id. If any error occur, the error is returned.
func (b *blackBearGobReader) GetItems(id int64, items ...interface{}) error {
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

//Public methods
//=============================================================================
//Get current size
func (db *blackBearDB) Size() int64 {
	return db.storage.Size()
}

//Make sure to close it before exit! Better use defer.
func (db *blackBearDB) Close() error {
	return db.storage.Close()
}

//=============================================================================
