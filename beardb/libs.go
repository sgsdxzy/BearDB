package beardb

import (
	"io"
)

//The abstract underlying storage for bearDBs
type BearStorage interface {
        io.WriterAt
        io.ReaderAt
        io.Closer
        Size() int64 //Return the current offset
}

//Wrap an io.WriterAt to a threadsafe io.Writer
//=============================================================================
func WrapWriter(w io.WriterAt, offset int64) io.Writer {
        return &wrappedWriter{w, offset}
}

type wrappedWriter struct {
        writerAt io.WriterAt
        offset int64
}

func (w *wrappedWriter) Write(p []byte) (n int, err error) {
        n,err = w.writerAt.WriteAt(p, w.offset)
        w.offset += int64(n)
        return
}

//Wrap an io.ReaderAt to a threadsafe io.Reader
//=============================================================================
func WrapReader(r io.ReaderAt, offset int64) io.Reader {
        return &wrappedReader{r, offset}
}

type wrappedReader struct {
        readerAt io.ReaderAt
        offset int64
}

func (w *wrappedReader) Read(p []byte) (n int, err error) {
        n, err = w.readerAt.ReadAt(p, w.offset)
        w.offset += int64(n)
        return
}

//=============================================================================
