package cckv

import (
	"fmt"
	"testing"
)

//func EntryEq(e1 Entry, e2 Entry) bool {
//	if e1.keySize != e2.keySize ||
//		e1.valSize != e2.valSize ||
//		e1.mask != e2.mask ||
//		e1.key
//}


func TestEntry(t *testing.T) {
	key := "liwei"
	val := "cckv"
	et := NewEntry([]byte(key), []byte(val))
	et.mask = 1
	buf := EnCode(et)

	res := DeCode(buf)
	reskey := make([]byte, res.keySize)
	resval := make([]byte, res.valSize)
	copy(reskey, buf[10:])
	copy(resval, buf[10+res.keySize:])
	res.key = reskey
	res.value = resval

	fmt.Println(et, res, buf)
}
