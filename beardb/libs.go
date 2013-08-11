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

//Re-input s at offset. The serialized size of s must be kept excatly the same
func (i *inputer) Reput(offset int64, s Serializer) {
        i.writer.Seek(offset, os.SEEK_SET)
        s.Serialize(i.writer)
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

