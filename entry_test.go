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
	resk := make([]byte, res.keySize)
	resv := make([]byte, res.valSize)
	copy(resk, buf[10:])
	copy(resv, buf[10+res.keySize:])
	res.key = resk
	res.value = resv

	fmt.Println(et, res, buf)
}
