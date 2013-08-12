package beardb

import (
	"io"
)

//The abstract underlying storage for bearDBs
type BearStorage interface {
	io.WriterAt
	io.ReaderAt
	io.Closer
	Size() int64 //Return the current Offset
}

//Wrap an io.WriterAt to a threadsafe io.Writer
//=============================================================================
type SafeWriter struct {
	io.WriterAt
	Offset int64
}

func (i *SafeWriter) Write(p []byte) (n int, err error) {
	n, err = i.WriteAt(p, i.Offset)
	i.Offset += int64(n)
	return
}

//Wrap an io.ReaderAt to a threadsafe io.Reader
//=============================================================================
type SafeReader struct {
	io.ReaderAt
	Offset int64
}

func (o *SafeReader) Read(p []byte) (n int, err error) {
	n, err = o.ReadAt(p, o.Offset)
	o.Offset += int64(n)
	return
}

//Old API
//=============================================================================
//Serialize writes serialized bytes to io.Writer and returns number of bytes wrote
//Deserialize reads bytes from io.Reader
//Serializers may always be pointers as Deserialize need to change the
//contents of it
type Serializer interface {
	Serialize(w io.Writer)
	Deserialize(r io.Reader)
}
