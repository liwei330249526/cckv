package cckv

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	db := OpenDb("/tmp/cckv")
	if db == nil {
		return
	}

	db.Put([]byte("li"), []byte("wei"))
	db.Put([]byte("li2"), []byte("wei2"))

	et1 := db.Get([]byte("li"))
	et2 := db.Get([]byte("li2"))

	k1 := string(et1.key)
	k2 := string(et2.key)
	v1 := string(et1.value)
	v2 := string(et2.value)
	fmt.Println(k1,k2,v1,v2)
}

/*
cat /tmp/cckv/cckv.data
liweiliwei2liwei3liwei4liwei5liwei6

cat /tmp/cckv/cckv.datat
liwei6
*/
func TestDB_Merge(t *testing.T) {
	db := OpenDb("/tmp/cckv")
	if db == nil {
		return
	}

	db.Put([]byte("li"), []byte("wei"))
	db.Put([]byte("li"), []byte("wei2"))
	db.Put([]byte("li"), []byte("wei3"))
	db.Put([]byte("li"), []byte("wei4"))
	db.Put([]byte("li"), []byte("wei5"))
	db.Put([]byte("li"), []byte("wei6"))
	db.Merge()
	et1 := db.Get([]byte("li"))

	k1 := string(et1.key)
	v1 := string(et1.value)
	fmt.Println(k1,v1)
}