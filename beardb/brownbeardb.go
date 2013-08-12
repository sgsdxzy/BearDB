package beardb

import (
	"bytes"
	"encoding/gob"
	"os"
)

const (
	infoLength = 32
	sizeLength = 4
)

func int64Abs(i int32) int64 {
	if i < 0 {
		return int64(-i)
	}
	return int64(i)
}

//A full-featured mutable database
type brownBearDB struct {
	Margin int //Margin between two entries
	path   string
	file   *os.File
	frag   map[int64]int32 //offset to size
	i      inputer
	o      outputer
}

//size info at id
func (db *brownBearDB) info(id int64) int32 {
	size := new(Int32Serializer)
	db.o.Output(id-sizeLength, size)
	return size.Get()
}

//Public methods
//=============================================================================
func NewBrownBearDB(path string) *brownBearDB {
	db := new(brownBearDB)
	db.path = path
	db.file, _ = os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	db.file.WriteAt([]byte("BrownBearDB 0.1"), 0)
	size, _ := db.file.Seek(0, os.SEEK_END)
	if size < infoLength { //Offset to write version info
		db.file.Truncate(infoLength)
	}
	db.i = inputer{db.file}
	db.o = outputer{db.file}
	fragfile, err := os.Open(db.path + ".frag")
	if err == nil { //Frag file does exist
		db.frag = make(map[int64]int32)
		decoder := gob.NewDecoder(fragfile)
		decoder.Decode(&(db.frag))
		fragfile.Close()
	}
	return db
}

//Retrun next id. If there is none, -1 is returned
//=============================================================================
func (db *brownBearDB) Next(id int64) (next int64) {
	if id < infoLength+sizeLength {
		return infoLength + sizeLength
	}

	current := id
	size := new(Int32Serializer) //Size of chunk

	db.o.Output(current-sizeLength, size)
	current += int64Abs(size.Get())
	if current > db.Size() {
		return -1
	}
	db.o.Output(current-sizeLength, size)
	for size.Get() < 0 {
		current += int64Abs(size.Get())
		if current > db.Size() {
			return -1
		}
		db.o.Output(current-sizeLength, size)
	}
	return current
}

//Defrag database from begin to end
//=============================================================================
func (db *brownBearDB) Defrag(begin, end int64) {
	if begin < infoLength+sizeLength {
		begin = infoLength + sizeLength
	}
	if end > db.Size() {
		end = db.Size()
	}
	size := new(Int32Serializer)
	nsize := new(Int32Serializer)

	for {
		db.o.Output(begin-sizeLength, size)
		for size.Get() > 0 { //begin has not been deleted
			begin += int64(size.Get())
			if begin > end {
				return
			}
			db.o.Output(begin-sizeLength, size)
		}
		current := begin + int64(-size.Get())
		if current > end {
			return
		}
		for {
			db.o.Output(current-sizeLength, nsize)
			if nsize.Get() < 0 {
				if db.FragInfo() {
					delete(db.frag, current)
				}
				size.Set(size.Get() + nsize.Get())
				current += int64(-nsize.Get())
				if current > end {
					if db.FragInfo() {
						db.frag[begin] = -size.Get()
					}
					db.i.InputAt(begin-sizeLength, size)
					return
				}
			} else {
				if db.FragInfo() {
					db.frag[begin] = -size.Get()
				}
				db.i.InputAt(begin-sizeLength, size)
				begin = current
				break
			}
		}
	}
}

//=============================================================================
func (db *brownBearDB) DisableFragInfo() {
	db.frag = nil
}

//Whether the map has fraginfo
//=============================================================================
func (db *brownBearDB) FragInfo() bool {
	return (db.frag != nil)
}

//=============================================================================
func (db *brownBearDB) CreateFragInfo() {
	db.frag = make(map[int64]int32)
	var current int64 = infoLength + sizeLength
	size := new(Int32Serializer)
	for current < db.Size() {
		db.o.Output(current-sizeLength, size)
		if size.Get() < 0 {
			db.frag[current] = -size.Get()
		}
		current += int64Abs(size.Get())
	}
}

func (db *brownBearDB) Size() int64 {
	size, _ := db.i.Size()
	return size
}

//AddEntry will try to insert entry to fragments large enough to hold it
//If there is no such fragment, it will be appended to the end of file
//=============================================================================
func (db *brownBearDB) AddEntry(key Serializer, value Serializer) int64 {
	var b bytes.Buffer
	value.Serialize(&b)
	key.Serialize(&b)
	var length int32 = int32(b.Len() + sizeLength)
	if db.FragInfo() {
		for id, size := range db.frag {
			if size >= length { //Enough to hold
				newsize := NewInt32Serializer(length)
				db.i.InputAt(id-sizeLength, newsize)
				db.file.Seek(id, os.SEEK_SET)
				b.WriteTo(db.file)
				newsize.Set(-(size - length))
				db.i.InputAt(id+int64(length)-sizeLength, newsize)
				delete(db.frag, id)
				db.frag[id+int64(length)+sizeLength] = size - sizeLength - length
				return id
			}
		}
	}

	//Have to append at the end
	return db.AppendEntry(key, value)
}

//AppendEntry puts the entry at the end of file
//If the key is not to be embeded, NilSerializer can be used for it. If key
//and value are both NilSerializer, the entry will be deleted by Defrag()
//The result: size + value + key + margin of bytes, with id points to value
//=============================================================================
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

//Modify key and value at id. The serialized size of value must not
//exceed the original size
//=============================================================================
func (db *brownBearDB) ReEntry(id int64, key Serializer, value Serializer) {
	var b bytes.Buffer
	value.Serialize(&b)
	key.Serialize(&b)
	var length int32 = int32(b.Len())
	size := new(Int32Serializer)
	db.o.Output(id-sizeLength, size)
	if length > (size.Get() - sizeLength) { //Cannot put
		return
	}
	//db.i.InputAt(id, value)
	db.file.Seek(id, os.SEEK_SET)
	b.WriteTo(db.file)
}

//Modify value at id. The serialized size of value must be exactly the same.
//=============================================================================
func (db *brownBearDB) Modify(id int64, value Serializer) {
	db.i.InputAt(id, value)
}

//Delete entry at id
//=============================================================================
func (db *brownBearDB) Delete(id int64) {
	size := new(Int32Serializer)
	db.o.Output(id-sizeLength, size)
	if size.Get() > 0 { //Not deleted
		if db.FragInfo() {
			db.frag[id] = size.Get()
		}
		size.Set(-size.Get())
		db.i.InputAt(id-sizeLength, size)
	}
}

//Get only the value
//=============================================================================
func (db *brownBearDB) GetValue(id int64, value Serializer) Serializer {
	return db.o.Output(id, value)
}

//Get both key and value
//=============================================================================
func (db *brownBearDB) GetKeyAndValue(id int64, key, value Serializer) (Serializer, Serializer) {
	db.o.reader.Seek(id, os.SEEK_SET)
	value.Deserialize(db.o.reader)
	key.Deserialize(db.o.reader)
	return key, value
}

//Make sure to close it before exit. Better use defer.
//=============================================================================
func (db *brownBearDB) Close() {
	if db.FragInfo() {
		fragfile, _ := os.Create(db.path + ".frag")
		encoder := gob.NewEncoder(fragfile)
		encoder.Encode(db.frag)
		fragfile.Close()
	}
	db.file.Close()
}

//=============================================================================
