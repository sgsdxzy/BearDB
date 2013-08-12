package beardb

import (
	"encoding/binary"
	"io"
)

var (
	Nil = new(nilSerializer) //Place holder that does nothing
)

// warpper of basic types

type nilSerializer struct{}
type Bool int8 //Must be fixed-size
type Byte uint8
type Int32 int32
type Int64 int64
type Float32 float32
type Float64 float64
type String []byte

//Implementations of  wrappers
//=============================================================================

//Do nothing
func (i *nilSerializer) Serialize(w io.Writer)   {}
func (i *nilSerializer) Deserialize(r io.Reader) {}

//=============================================================================

func NewBool(n bool) *Bool {
	var i Bool
	if n {
		i = Bool(1)
	} else {
		i = Bool(0)
	}
	return &i
}

func (i *Bool) Set(n bool) {
	if n {
		*i = Bool(1)
	} else {
		*i = Bool(0)
	}
}

func (i *Bool) Get() bool {
	if *i == 1 {
		return true
	} else {
		return false
	}
}

func (i *Bool) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Bool) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewByte(n byte) *Byte {
	r := Byte(n)
	return &r
}

func (i *Byte) Set(n byte) {
	*i = Byte(n)
}

func (i *Byte) Get() byte {
	return byte(*i)
}

func (i *Byte) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Byte) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewInt32(n int32) *Int32 {
	r := Int32(n)
	return &r
}

func (i *Int32) Set(n int32) {
	*i = Int32(n)
}

func (i *Int32) Get() int32 {
	return int32(*i)
}

func (i *Int32) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Int32) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewInt64(n int64) *Int64 {
	r := Int64(n)
	return &r
}

func (i *Int64) Set(n int64) {
	*i = Int64(n)
}

func (i *Int64) Get() int64 {
	return int64(*i)
}

func (i *Int64) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Int64) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewFloat32(n float32) *Float32 {
	r := Float32(n)
	return &r
}

func (i *Float32) Set(n float32) {
	*i = Float32(n)
}

func (i *Float32) Get() float32 {
	return float32(*i)
}

func (i *Float32) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Float32) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewFloat64(n float64) *Float64 {
	r := Float64(n)
	return &r
}

func (i *Float64) Set(n float64) {
	*i = Float64(n)
}

func (i *Float64) Get() float64 {
	return float64(*i)
}

func (i *Float64) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, i)
}

func (i *Float64) Deserialize(r io.Reader) {
	binary.Read(r, binary.LittleEndian, i)
}

//=============================================================================

func NewString(n string) *String {
	r := String(n)
	return &r
}

func (s *String) Set(n string) {
	*s = String(n)
}

func (s *String) Get() string {
	return string(*s)
}

func (s *String) Serialize(w io.Writer) {
	NewInt32(int32(len(*s))).Serialize(w)
	binary.Write(w, binary.LittleEndian, s)
}

func (s *String) Deserialize(r io.Reader) {
	length := new(Int32)
	length.Deserialize(r)
	*s = make(String, int(length.Get()))
	binary.Read(r, binary.LittleEndian, s)
}

//=============================================================================
