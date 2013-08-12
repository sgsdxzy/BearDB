package beardb

import (
	"io"
	"os"
)

//Serialize writes serialized bytes to io.Writer and returns number of bytes wrote
//Deserialize reads bytes from io.Reader
//Serializers may always be pointers as Deserialize need to change the
//contents of it
type Serializer interface {
	Serialize(w io.Writer)
	Deserialize(r io.Reader)
}

//=============================================================================
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

//Put a series of Serializer into the end. Id of first item is returned.
func (i *inputer) Inputs(items ...Serializer) int64 {
	start, _ := i.writer.Seek(0, os.SEEK_END)
	for _, s := range items {
		s.Serialize(i.writer)
	}
	return start
}

//Re-input s at offset. The serialized size of s must be kept excatly the same
func (i *inputer) InputAt(offset int64, s Serializer) {
	i.writer.Seek(offset, os.SEEK_SET)
	s.Serialize(i.writer)
}

//Re-input a series of items at offset. The serialized size of items must
//be kept excatly the same
func (i *inputer) InputsAt(offset int64, items ...Serializer) {
	i.writer.Seek(offset, os.SEEK_SET)
	for _, s := range items {
		s.Serialize(i.writer)
	}
}

//The current size
func (i *inputer) Size() (ret int64, err error) {
	return i.writer.Seek(0, os.SEEK_END)
}

//=============================================================================
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

//Reads data at offset to a series of Serializers
func (o *outputer) Outputs(offset int64, items ...Serializer) {
	o.reader.Seek(offset, os.SEEK_SET)
	for _, s := range items {
		s.Deserialize(o.reader)
	}
}

//The current size
func (o *outputer) Size() (ret int64, err error) {
	return o.reader.Seek(0, os.SEEK_END)
}

//=============================================================================
