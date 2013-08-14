package beardb

import (
	"os"
)

//Simple wrapper for file
type raccoon struct {
	*os.File
}

func NewRaccoon(path string) *raccoon {
	file, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	return &raccoon{file}
}

func (r *raccoon) Size() int64 {
        size, _ := r.Seek(0, os.SEEK_END)
	return size
}
