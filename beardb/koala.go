package beardb

import (
        "errors"
        "os"
        )

type koala []byte

func NewKoala(size int) *koala {
        k := make(koala, 0, size)
        return &k
}

func (k *koala) WriteAt(p []byte, off int64) (n int, err error) {
        if len(*k) - int(off) >= len(p) { //Enough space to write
                n = copy((*k)[off:], p)
                if n < len(p) {
                        err = errors.New("Koala Copy error")
                }
        } else {
                *k = append((*k)[:off], p...)
                n = len(p)
        }
        return
}

func (k *koala) ReadAt(p []byte, off int64) (n int, err error) {
        n = copy(p, (*k)[off:])
        if n < len(p) {
                err = errors.New("Insufficient bytes")
        }
        return
}

func (k *koala) Close() error {
        *k = nil
        return nil
}

func (k *koala) Size() int64 {
        return int64(len(*k))
}

func (k *koala) ToFile(path string) error {
        file, err := os.Create(path)
        if err != nil {
                return err
        }
        defer file.Close()
        _, err = file.Write([]byte(*k))
        return err
}

func (k *koala) FromFile(path string) error {
        file, err := os.Open(path)
        if err != nil {
                return err
        }
        defer file.Close()
        fi, err := file.Stat()
        if err != nil {
                return err
        }
        *k = make(koala, fi.Size())
        _, err = file.Read([]byte(*k))
        return err
}
