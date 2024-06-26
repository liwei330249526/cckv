package cckv

import (
	"encoding/binary"
	"hash/crc32"
)

var (
	EntryHeadSize = 18
)


type Entry struct {
	crc32   uint32 // 4
	txnId   uint32 // 4
	keySize uint32 // 4
	valSize uint32 // 4
	mask uint16 // 2
	key []byte
	value []byte
}

// NewEntry 新建 entry
func NewEntry(key []byte, val []byte) *Entry {
	e := &Entry{
		crc32: crc32.ChecksumIEEE(val),
		txnId: 1,
		keySize: uint32(len(key)),
		valSize: uint32(len(val)),
		key:     key,
		value:   val,
	}
	return e
}

func (e *Entry)Size() int {
	res := 0
	res += EntryHeadSize
	res += int(e.keySize)
	res += int(e.valSize)
	return res
}

// EnCode 将entry编码为 []byte
func EnCode(entry *Entry) []byte {
	buf := make([]byte, uint32(EntryHeadSize)+entry.keySize+entry.valSize)
	//binary.BigEndian.AppendUint32(buf, entry.keySize)  // append 是不对的
	//binary.BigEndian.AppendUint32(buf[4:], entry.keySize)
	//binary.BigEndian.AppendUint16(buf[8:], entry.mask)

	//c32 := crc32.ChecksumIEEE(entry.value)
	binary.BigEndian.PutUint32(buf, entry.crc32)
	//binary.BigEndian.PutUint32(buf[4:], entry.txnId)
	binary.BigEndian.PutUint32(buf[4:], entry.txnId)
	binary.BigEndian.PutUint32(buf[8:], entry.keySize)
	binary.BigEndian.PutUint32(buf[12:], entry.valSize)
	binary.BigEndian.PutUint16(buf[16:], entry.mask)

	copy(buf[uint32(EntryHeadSize):], entry.key)
	copy(buf[uint32(EntryHeadSize)+entry.keySize:],entry.value)
	return buf
}

// DeCode 将 []byte 解码为entry，注意只有头部的10字节
func DeCode(buf []byte) *Entry {
	et := &Entry{
		crc32: binary.BigEndian.Uint32(buf),
		txnId: binary.BigEndian.Uint32(buf[4:]),
		keySize: binary.BigEndian.Uint32(buf[8:]),
		valSize: binary.BigEndian.Uint32(buf[12:]),
		mask: binary.BigEndian.Uint16(buf[16:]),
	}
	return et
}