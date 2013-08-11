package beardb

import (
        "os"
        "encoding/gob"
)

const (
        infoLength = 32
)

func int64Abs (i int64) int64 {
        if i < 0 {
                return -i
        }
        return i
}

//A full-featured mutable database
type brownBearDB struct {
        Margin int //Margin between two entries
        file *os.File
        frag map[int64]int32
        i inputer
        o outputer
}

func NewBrownBearDB(path string) *brownBearDB {
        db := new(brownBearDB)
	db.file, _ = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
        db.file.WriteAt([]byte("BrownBearDB 0.1"), 0)
        size, _:= db.file.Seek(0, os.SEEK_END)
        if (size < infoLength) { //Offset to write version info
                db.file.Truncate(infoLength)
        }
	db.i = inputer{db.file}
	db.o = outputer{db.file}
        db.frag = make(map[int64]int32)
        fragfile, err := os.Open(path+".frag")
        if err == nil { //Frag file does exist
                decoder := gob.NewDecoder(fragfile)
                decoder.Decode(&(db.frag))
        }
	return db
}

//Retrun next id. If there is none, -1 is returned
func (db *brownBearDB) Next(id int64) (next int64) {
        if (id < infoLength + 4) {
                return infoLength + 4
        }

        current := id
        size := new(Int32Serializer) //Size of chunk

        db.o.Output(current - 4, size)
        current += int64Abs(int64(size.Get()))
        if current > db.Size() {
                return -1
        }
        db.o.Output(current - 4, size)
        for size.Get() < 0 {
                current += int64Abs(int64(size.Get()))
                if current > db.Size() {
                        return -1
                }
                db.o.Output(current - 4, size)
        }
        return current
}

//Defrag database from begin to end
func (db *brownBearDB) Defrag(begin, end int64) {
        if (begin < infoLength + 4) {
                begin = infoLength + 4
        }
        if (end > db.Size()) {
                end = db.Size()
        }
        start := begin
        size := new(Int32Serializer)
        nsize := new(Int32Serializer)

        //Finding start
        db.o.Output(begin - 4, size)
        for size.Get() < 0 {
                start += int64Abs(int64(size.Get()))
                if start > end {
                        return
                }
                db.o.Output(start - 4, size)
        }

        current := start + int64Abs(int64(size.Get()))
        if current > end {
                return
        }
        for {
                for {
                        db.o.Output(current - 4, nsize)
                        if nsize.Get() < 0 {
                                size.Set(size.Get() +- nsize.Get())
                                current += int64(-nsize.Get())
                                if (current > end) {
                                        return
                                }
                        } else {
                                db.i.InputAt(start - 4, size)
                                start = current
                                break
                        }
                }
                current = start + int64(nsize.Get())
                if current > end {
                        return
                }
        }
}

func (db *brownBearDB) Size() int64 {
        size, _ := db.i.Size()
        return size
}

//AppendEntry puts the entry at the end of file
//If the key is not to be embeded, NilSerializer can be used for it. If key
//and value are both NilSerializer, the entry will be deleted by Defrag()
//The result: size + value + key + margin of bytes, with id points to value
func (db *brownBearDB) AppendEntry(key Serializer, value Serializer) int64 {
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

//Modify value at id. The serialized size of value must be exactly the same, 
//or safety cannot be guaranteed.
func (db *brownBearDB) ReEntry(id int64, value Serializer) {
        db.i.InputAt(id, value)
}

//Delete entry at id
func (db *brownBearDB) Delete(id int64) {
        size := new(Int32Serializer)
        db.o.Output(id - 4, size)
        if size.Get() > 0 { //Not deleted
                size.Set(-size.Get())
                db.i.InputAt(id - 4, size)
        }
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
