package beardb

import (
        "encoding/gob"
)

//The compact and most simplified append-and-read-only database
//=============================================================================
type blackBearDB struct {
        storage BearStorage
        encoder *gob.Encoder //Encoder for appending
}

func NewBlackBearDB(s BearStorage) *blackBearDB {
        return &blackBearDB{s, gob.NewEncoder(WrapWriter(s,s.Size()))}
}

//Internal libs
//=============================================================================
func (db *blackBearDB) encodeAt (e interface{}, offset int64) error {
        return gob.NewEncoder(WrapWriter(db.storage, offset)).Encode(e)
}

func (db *blackBearDB) decodeAt (e interface{}, offset int64) error {
        return gob.NewDecoder(WrapReader(db.storage, offset)).Decode(e)
}

//Public methods
//=============================================================================
//Get current size
func (db *blackBearDB) Size() int64 {
        return db.storage.Size()
}

//Append item to the end of storage. Item id is returned
func (db *blackBearDB) AddItem(item interface{}) (id int64, err error) {
        id = db.Size()
        return id, db.encoder.Encode(item)
}

//Append items to the end of storage. Id of first item and first error(if any)
//is returned
func (db *blackBearDB) AddItems(items... interface{}) (id int64, err error) {
        id = db.Size()
        for _, item := range items {
                err = db.encoder.Encode(item)
                if err != nil {
                        break
                }
        }
        return id, err
}

//Modify value at id. The serialized size of value must be the same or less.
//It could be very dangerous and generally discoraged
func (db *blackBearDB) Modify(id int64, item interface{}) error {
        return db.encodeAt(item, id)
}

//Get item at id
func (db *blackBearDB) GetItem(id int64, item interface{}) error {
	return db.decodeAt(item, id)
}

//Get items starting from id. If any error occur, the error is returned.
func (db *blackBearDB) GetItems(id int64, items... interface{}) error {
        var err error = nil
        decoder := gob.NewDecoder(WrapReader(db.storage, id))
        for _, item := range items {
                err = decoder.Decode(item)
                if err != nil {
                        break
                }
        }
	return err
}

//Make sure to close it before exit! Better use defer.
func (db *blackBearDB) Close() error {
	return db.storage.Close()
}

//=============================================================================
