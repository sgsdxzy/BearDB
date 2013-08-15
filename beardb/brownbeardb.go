package beardb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"sync"
)

//Data Structures
/*=============================================================================
The database starts with databaseinfoLength bytes of DataBase description,
followed be the first Block.

DataEntry:
----------------------------------------
|datainfo|           data              |
----------------------------------------
If Deleted in datainfo is true, then this entry has been deleted.
If LongJump is true, then the following data is an int64, pointing to the start
of real data.

Chunk:
---------------------------------------------------
|Chunkinfo|DataEntry|DataEntry|(deleted)|DataEntry|
---------------------------------------------------
Chunk is the minimum unit of space allocation.
Start of one DataEntry + Length in datainfo = start of next DataEntry.
If a DataEntry is larger than chunksize, it must start from the begining of
a chunk, and the chunk is extended to a multiple of chunksize.

Block:
----------------------------------------------
||Chunk|Chunk|Extended Chunk|Chunk|Blockinfo||
----------------------------------------------
Block is the unit of RWMutex.
Start of one Chunk + Length in chunkinfo * chunksize = start of next Chunk.
If a DataEntry is larger than blocksize(which should be avoided), it must be
splited into multiple chunks, and Extend in blockinfo should be set to true:
--------------------------------------------
|| Huge Chunk |Blockinfo|| Split Chunk | ....
--------------------------------------------
Chunksize in HugeChunk is the total chunk size, whil chunksize in SplitChunk
is the remaining size. It is the same with data Length.

=============================================================================*/
const (
	databaseinfoLength = 32
	datainfoLength     = 4
	chunkinfoLength    = 1
	blockinfoLength    = 4
)

//(Deleted flag bit)(LongJump flag bit)(30-bits unsigned int of length)
type datainfo uint32

func newDataInfo(length int) *datainfo {
	info := new(datainfo)
	info.SetLength(length)
	return info
}

func (d *datainfo) SetDeleted(deleted bool) {
	if deleted {
		*d = *d | 2147483648
	} else {
		*d = *d & 2147483647
	}
}

func (d *datainfo) SetLongJump(longjump bool) {
	if longjump {
		*d = *d | 1073741824
	} else {
		*d = *d & 3221225471
	}
}

func (d *datainfo) SetLength(length int) {
	*d = (*d & 3221225472) + datainfo(length&1073741823)
}

func (d *datainfo) IsDeleted() bool {
	return ((*d) >> 31) == 1
}

func (d *datainfo) IsLongJump() bool {
	return (((*d) >> 30) & 1) == 1
}

func (d *datainfo) GetLength() int {
	return int((*d) & 1073741823)
}

func (d *datainfo) WriteTo(w io.Writer) error {
	return binary.Write(w, binary.LittleEndian, d)
}

func (d *datainfo) ReadFrom(r io.Reader) error {
	return binary.Read(r, binary.LittleEndian, d)
}

//A full-featured mutable database
//=============================================================================
type brownBearDB struct {
	storage BearStorage
	rwlock  sync.RWMutex //Appending lock
}

//Non-locking getting size
func (db *brownBearDB) size() int64 {
	return db.storage.Size()
}

//Public methods
//=============================================================================
//Constructor
func NewBrownBearDB(s BearStorage) *brownBearDB {
	return &brownBearDB{storage: s}
}

//Get current size
func (db *brownBearDB) Size() int64 {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	return db.size()
}

//Make sure to close it before exit! Better use defer.
func (db *brownBearDB) Close() error {
	return db.storage.Close()
}

//New Gob Writer. Create one for every thread doing writing
//=============================================================================
type brownBearGobWriter struct {
	buff *bytes.Buffer
	w    *SafeReadWriter
	e    *gob.Encoder
	db   *brownBearDB
}

func (db *brownBearDB) NewGobWriter() *brownBearGobWriter {
	b := new(brownBearGobWriter)
	b.buff = new(bytes.Buffer)
	b.w = &SafeReadWriter{db.storage, 0}
	b.e = gob.NewEncoder(b.buff)
	b.db = db
	return b
}

//Append item to the end of storage. Id and error(if any) is returned
func (b *brownBearGobWriter) AddItem(item interface{}) (id int64, err error) {
	err = b.e.Encode(item)
	info := newDataInfo(b.buff.Len())

	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	info.WriteTo(b.w)
	b.buff.WriteTo(b.w)

	return
}

//Append items to the end of storage. Id of first item and first error(if any)
//encountered is returned
func (b *brownBearGobWriter) AddItems(items ...interface{}) (id int64, err error) {
	for _, item := range items {
		err = b.e.Encode(item)
		if err != nil {
			return -1, err
		}
	}
	info := newDataInfo(b.buff.Len())

	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()

	id = b.db.size()
	b.w.Offset = id
	info.WriteTo(b.w)
	b.buff.WriteTo(b.w)
	return
}

//Modify items at id
func (b *brownBearGobWriter) Modify(id int64, items ...interface{}) error {
	var err error
	for _, item := range items {
		err = b.e.Encode(item)
		if err != nil {
			return err
		}
	}
	oldinfo := new(datainfo)
	b.w.Offset = id
	oldinfo.ReadFrom(b.w)

	b.db.rwlock.Lock()
	defer b.db.rwlock.Unlock()
	if b.buff.Len() <= oldinfo.GetLength() { //Can fit
		info := newDataInfo(oldinfo.GetLength())
		b.w.Offset = id
		info.WriteTo(b.w)
		b.buff.WriteTo(b.w)
	} else {
		if oldinfo.IsLongJump() {
			newid := new(Int64)
			newid.Deserialize(b.w)
			b.w.Offset = newid.Get()
			jumpedinfo := new(datainfo)
			jumpedinfo.ReadFrom(b.w)
			if b.buff.Len() <= jumpedinfo.GetLength() { //Can fit
				b.buff.WriteTo(b.w)
				return err
			}
		} else { //Allocate a new area for LongJump
			newid := b.db.size()
			info := newDataInfo(b.buff.Len())
			b.w.Offset = newid
			info.WriteTo(b.w)
			b.buff.WriteTo(b.w)
			//Set LongJump
			oldinfo.SetLongJump(true)
			b.w.Offset = id
			oldinfo.WriteTo(b.w)
			NewInt64(newid).Serialize(b.w)
		}
	}

	return err
}

//Get the underlying DB
func (b *brownBearGobWriter) GetDB() *brownBearDB {
	return b.db
}

//New Gob Reader. Create one for every thread doing reading
//=============================================================================
type brownBearGobReader struct {
	r  *SafeReader
	d  *gob.Decoder
	db *brownBearDB
}

func (db *brownBearDB) NewGobReader() *brownBearGobReader {
	b := new(brownBearGobReader)
	b.r = &SafeReader{db.storage, 0}
	b.d = gob.NewDecoder(b.r)
	b.db = db
	return b
}

//Get item at id
func (b *brownBearGobReader) GetItem(id int64, item interface{}) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()

	info := new(datainfo)
	b.r.Offset = id
	info.ReadFrom(b.r)
	if info.IsLongJump() {
		newid := new(Int64)
		newid.Deserialize(b.r)
		b.r.Offset = newid.Get() + datainfoLength
	}
	return b.d.Decode(item)
}

//Get items starting from id. If any error occur, the error is returned.
func (b *brownBearGobReader) GetItems(id int64, items ...interface{}) error {
	b.db.rwlock.RLock()
	defer b.db.rwlock.RUnlock()

	info := new(datainfo)
	b.r.Offset = id
	info.ReadFrom(b.r)
	if info.IsLongJump() {
		newid := new(Int64)
		newid.Deserialize(b.r)
		b.r.Offset = newid.Get() + datainfoLength
	}

	var err error = nil
	for _, item := range items {
		err = b.d.Decode(item)
		if err != nil {
			break
		}
	}
	return err
}

//Get the underlying DB
func (b *brownBearGobReader) GetDB() *brownBearDB {
	return b.db
}

//=============================================================================
