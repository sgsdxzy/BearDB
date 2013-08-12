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

//Serializer API
//=============================================================================
//Serialize writes serialized bytes to io.Writer and returns any error
//encountered. Deserialize reads bytes from io.Reader and re-construct the
//item. Serializers may always be pointers as Deserialize need to change the
//contents of it
type Serializer interface {
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error
}
