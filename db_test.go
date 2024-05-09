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
	db := OpenDb("/tmp/cckv", CecheSizeDefalut)
	if db == nil {
		return
	}

	db.Put([]byte("li"), []byte("wei"))
	db.Put([]byte("li2"), []byte("wei2"))

	v1 := db.Get([]byte("li"))
	v2 := db.Get([]byte("li2"))
	fmt.Println(v1,v2)
}

/*
cat /tmp/cckv/cckv.data
liweiliwei2liwei3liwei4liwei5liwei6

cat /tmp/cckv/cckv.datat
liwei6
*/
func TestDB_Merge(t *testing.T) {
	db := OpenDb("/tmp/cckv", CecheSizeDefalut)
	if db == nil {
		return
	}

	db.Put([]byte("mydb"), []byte("cckv"))
	//db.Put([]byte("mydb"), []byte("cckv2"))
	//db.Put([]byte("mydb"), []byte("cckv3"))
	//db.Put([]byte("mydb"), []byte("cckv4"))
	//db.Put([]byte("mydb"), []byte("cckv5"))
	//db.Put([]byte("mydb"), []byte("cckv6"))
	db.Merge()
	v1 := db.Get([]byte("mydb"))

	fmt.Println(v1)

	db.Del([]byte("mydb"))
	db.Merge()
	v2 := db.Get([]byte("mydb"))
	fmt.Println(v2)
}

func TestDB_Merge1(t *testing.T) {
	dir := TempDir()
	db := OpenDb(dir, CecheSizeDefalut)
	db.Put([]byte("key1"), []byte("val1"))
	db.Put([]byte("key2"), []byte("val1"))

	db.Del([]byte("key1"))
	db.Del([]byte("key2"))

	db.Merge()


	db.DropDB()
}
