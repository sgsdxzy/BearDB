package beardb

import (
	"encoding/binary"
	"io"
	"os"
)

var (
	NullSerializer = new(nullSerializer) //Place holder that does nothing
)

//Serializer warpper of basic types

type nullSerializer struct{}
type Int32Serializer int32
type Int64Serializer int64
type Float32Serializer float32
type Float64Serializer float64

//Serialize writes serialized bytes to io.Writer and returns number of bytes wrote
//Deserialize reads bytes from io.Reader
//Serializers may always be pointers as Deserialize need to change the
//contents of it
type Serializer interface {
	Serialize(w io.Writer)
	Deserialize(r io.Reader)
}

//Inputer for Bear DataBase
type inputer struct {
	writer io.WriteSeeker
}

func NewInputer(w io.WriteSeeker) *inputer {
	return &inputer{w}
}

//Put s into database, numeric item id is returned
func (i *inputer) Input(s Serializer) int64 {
	start, _ := i.writer.Seek(0, os.SEEK_END)
	s.Serialize(i.writer)
	return start
}

//Outputer for Bear DataBase
type outputer struct {
	reader io.ReadSeeker
}

func NewOutputer(r io.ReadSeeker) *outputer {
	return &outputer{r}
}

//Reads data at offset to s
func (o *outputer) Output(offset int64, s Serializer) Serializer {
	o.reader.Seek(offset, os.SEEK_SET)
	s.Deserialize(o.reader)
	return s
}

//The compact and most simplified append-and-read-only database
//=============================================================================
type blackBearDB struct {
	file  *os.File
	embed bool //Whether embed keys in db
	i     inputer
	o     outputer
}

func NewBlackBearDB(path string, e bool) *blackBearDB {
	db := new(blackBearDB)
	db.file, _ = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	db.embed = e
	db.i = inputer{db.file}
	db.o = outputer{db.file}
	return db
}

//Have this DB embedded keys?
func (db *blackBearDB) Embed() bool {
	return db.embed
}

//If the key is not to be embeded, NullSerializer can be used for it
func (db *blackBearDB) AddEntry(key Serializer, value Serializer) int64 {
	id := db.i.Input(value)
	if db.embed {
		db.i.Input(key)
	}
	return id
}

func (db *blackBearDB) GetValue(id int64, value Serializer) Serializer {
	return db.o.Output(id, value)
}

//If embed==false, key is not changed and directly returned
func (db *blackBearDB) GetKeyAndValue(id int64, key, value Serializer) (Serializer, Serializer) {
	db.o.reader.Seek(id, os.SEEK_SET)
	value.Deserialize(db.o.reader)
	if db.embed {
		key.Deserialize(db.o.reader)
	}
	return key, value
}

//Make sure to close it before exit!
func (db *blackBearDB) Close() {
	db.file.Close()
}

//A full-featured mutable database
//=============================================================================

//Implementations of Serializer wrappers
//=============================================================================

//Do nothing
func (i *nullSerializer) Serialize(w io.Writer)   {}
func (i *nullSerializer) Deserialize(r io.Reader) {}

//=============================================================================

func (i *Int32Serializer) Set(n int32) {
	*i = Int32Serializer(n)
}

func (i *Int32Serializer) Get() int32 {
	return int32(*i)
}

func (i *Int32Serializer) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Int32Serializer) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func (i *Int64Serializer) Set(n int64) {
	*i = Int64Serializer(n)
}

func (i *Int64Serializer) Get() int64 {
	return int64(*i)
}

func (i *Int64Serializer) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Int64Serializer) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func (i *Float32Serializer) Set(n float32) {
	*i = Float32Serializer(n)
}

func (i *Float32Serializer) Get() float32 {
	return float32(*i)
}

func (i *Float32Serializer) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Float32Serializer) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func (i *Float64Serializer) Set(n float64) {
	*i = Float64Serializer(n)
}

func (i *Float64Serializer) Get() float64 {
	return float64(*i)
}

func (i *Float64Serializer) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Float64Serializer) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================
