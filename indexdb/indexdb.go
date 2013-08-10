package indexdb

import (
	"encoding/binary"
	"io"
)

//Serializer warpper of basic types

type Int32Serializer int32
type Int64Serializer int64
type Float32Serializer float32
type Float64Serializer float64

//Serialize writes serialized bytes to io.Writer and returns number of bytes wrote
//Deserialize reads bytes from io.Reader
type Serializer interface {
	Serialize(w io.Writer) int64
	Deserialize(r io.Reader)
}

//Inputer for Index DataBase
type inputer struct {
	offset int64
	writer io.WriteSeeker
}

func NewInputer(w io.WriteSeeker) *inputer {
	return &inputer{0, w}
}

//Put s into database, numeric item id is returned
func (i *inputer) Input(s Serializer) int64 {
	start := i.offset
	i.writer.Seek(start, 0)
	i.offset += s.Serialize(i.writer)
	return start
}

//Get current offset
func (i *inputer) GetOffset() int64 {
	return i.offset
}

//Outputer for Index DataBase
type outputer struct {
	reader io.ReadSeeker
}

func NewOutputer(r io.ReadSeeker) *outputer {
	return &outputer{r}
}

//Reads data at offset to s
func (o *outputer) Output(s Serializer, offset int64) {
	o.reader.Seek(offset, 0)
	s.Deserialize(o.reader)
}

//Implementations of Serializer wrappers
//=============================================================================

func (i *Int32Serializer) Set(n int32) {
	*i = Int32Serializer(n)
}

func (i *Int32Serializer) Get() int32 {
	return int32(*i)
}

func (i *Int32Serializer) Serialize(w io.Writer) int64 {
	binary.Write(w, binary.LittleEndian, i)
	return int64(binary.Size(i))
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

func (i *Int64Serializer) Serialize(w io.Writer) int64 {
	binary.Write(w, binary.LittleEndian, i)
	return int64(binary.Size(i))
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

func (i *Float32Serializer) Serialize(w io.Writer) int64 {
	binary.Write(w, binary.LittleEndian, i)
	return int64(binary.Size(i))
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

func (i *Float64Serializer) Serialize(w io.Writer) int64 {
	binary.Write(w, binary.LittleEndian, i)
	return int64(binary.Size(i))
}

func (i *Float64Serializer) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================
