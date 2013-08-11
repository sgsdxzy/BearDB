package beardb

import (
        "os"
)

//A full-featured mutable database
type brownBearDB struct {
        Margin int //Margin between two entries
        file *os.File
        i inputer
        o outputer
}

func NewBrownBearDB(path string) *brownBearDB {
        db := new(brownBearDB)
	db.file, _ = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
        db.file.WriteAt([]byte("BrownBearDB 0.1"), 0)
        size, _:= db.file.Seek(0, os.SEEK_END)
        if (size < 32) { //Offset to write version info
                db.file.Truncate(32)
        }
	db.i = inputer{db.file}
	db.o = outputer{db.file}
	return db
}

//If the key is not to be embeded, NilSerializer can be used for it
//The result: size + value + key + margin of bytes, and id points to value
func (db *brownBearDB) AddEntry(key Serializer, value Serializer) int64 {
        size := NewInt32Serializer(0)
        start := db.i.Input(size)
	id := db.i.Input(value)
        db.i.Input(key)
        mid, _ := db.i.Size()
        db.file.Truncate(mid + int64(db.Margin))
        end, _ := db.i.Size()
        size.Set(int32(end - start))
        db.i.InputAt(start, size)
	return id
}

//Modify value at id. The serialized size of value must be exactly the same.
func (db *brownBearDB) ReEntry(id int64, value Serializer) {
        db.i.InputAt(id, value)
}

//Get only the value
func (db *brownBearDB) GetValue(id int64, value Serializer) Serializer {
	return db.o.Output(id, value)
}

//Get both key and value
func (db *brownBearDB) GetKeyAndValue(id int64, key, value Serializer) (Serializer, Serializer) {
	db.o.reader.Seek(id, os.SEEK_SET)
	value.Deserialize(db.o.reader)
        key.Deserialize(db.o.reader)
	return key, value
}

//Make sure to close it before exit! Better use defer.
func (db *brownBearDB) Close() {
	db.file.Close()
}


//=============================================================================
