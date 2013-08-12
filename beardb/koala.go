package beardb

import (
        "errors"
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
