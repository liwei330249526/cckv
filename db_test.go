package cckv

import (
	"fmt"
	//"io/ioutil"
	//"os"
	//"path/filepath"
	//"sort"
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

	db.Put([]byte("mydb"), []byte("cckv"))
	db.Put([]byte("mydb"), []byte("cckv2"))
	db.Put([]byte("mydb"), []byte("cckv3"))
	db.Put([]byte("mydb"), []byte("cckv4"))
	db.Put([]byte("mydb"), []byte("cckv5"))
	db.Put([]byte("mydb"), []byte("cckv6"))
	db.Merge()
	et1 := db.Get([]byte("mydb"))

	k1 := string(et1.key)
	v1 := string(et1.value)
	fmt.Println(k1,v1)

	db.Del([]byte("mydb"))
	db.Merge()
	et2 := db.Get([]byte("mydb"))
	fmt.Println(et2)
}

func TestDB_Merge1(t *testing.T) {
	dir := TempDir()
	db := OpenDb(dir)
	db.Put([]byte("key1"), []byte("val1"))
	db.Put([]byte("key2"), []byte("val1"))

	db.Del([]byte("key1"))
	db.Del([]byte("key2"))

	db.Merge()


	db.DropDB()
}
