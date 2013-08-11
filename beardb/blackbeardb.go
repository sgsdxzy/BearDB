package beardb

import (
	"os"
)

//The compact and most simplified append-and-read-only database
//=============================================================================
type blackBearDB struct {
	file  *os.File
	i     inputer
	o     outputer
}

func NewBlackBearDB(path string) *blackBearDB {
	db := new(blackBearDB)
	db.file, _ = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	db.i = inputer{db.file}
	db.o = outputer{db.file}
	return db
}

//If the key is not to be embeded, NilSerializer can be used for it
func (db *blackBearDB) AddEntry(key Serializer, value Serializer) int64 {
	id := db.i.Input(value)
        db.i.Input(key)
	return id
}

//Modify value at id. The serialized size of value must be exactly the same.
func (db *blackBearDB) ReEntry(id int64, value Serializer) {
        db.i.InputAt(id, value)
}

//Get only the value
func (db *blackBearDB) GetValue(id int64, value Serializer) Serializer {
	return db.o.Output(id, value)
}

//Get both key and value
func (db *blackBearDB) GetKeyAndValue(id int64, key, value Serializer) (Serializer, Serializer) {
	db.o.reader.Seek(id, os.SEEK_SET)
	value.Deserialize(db.o.reader)
        key.Deserialize(db.o.reader)
	return key, value
}

//Make sure to close it before exit! Better use defer.
func (db *blackBearDB) Close() {
	db.file.Close()
}

//=============================================================================


