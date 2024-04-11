package cckv

import (
	"fmt"
	"testing"
)

func TestDbFile(t *testing.T) {
	dbFile := OpenFile("/tmp/cckv/cckv.data")

	if dbFile == nil {
		return
	}

	key := "liwei"
	val := "cckv"
	et := NewEntry([]byte(key), []byte(val))
	et.mask = 1

	err := dbFile.WriteFile(et)
	if err != nil {
		return
	}

	key2 := "liwei2"
	val2 := "cckv2"
	et2:= NewEntry([]byte(key2), []byte(val2))
	et2.mask = 1
	err = dbFile.WriteFile(et2)
	if err != nil {
		return
	}
	pos := 0
	err, resEt1 := dbFile.ReadFile(pos)
	if resEt1 == nil {
		return
	}

	pos += resEt1.Size()

	err, resEt2 := dbFile.ReadFile(pos)
	if resEt2 == nil {
		return
	}

	fmt.Println(string(resEt1.key), string(resEt1.value))
	fmt.Println(string(resEt2.key), string(resEt2.value))
}