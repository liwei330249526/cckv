package cckv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
//*/
//func TestDB_Merge(t *testing.T) {
//	db := OpenDb("/tmp/cckv")
//	if db == nil {
//		return
//	}
//
//	db.Put([]byte("mydb"), []byte("cckv"))
//	db.Put([]byte("mydb"), []byte("cckv2"))
//	db.Put([]byte("mydb"), []byte("cckv3"))
//	db.Put([]byte("mydb"), []byte("cckv4"))
//	db.Put([]byte("mydb"), []byte("cckv5"))
//	db.Put([]byte("mydb"), []byte("cckv6"))
//	db.Merge()
//	et1 := db.Get([]byte("mydb"))
//
//	k1 := string(et1.key)
//	v1 := string(et1.value)
//	fmt.Println(k1,v1)
//
//	db.Del([]byte("mydb"))
//	db.Merge()
//	et2 := db.Get([]byte("mydb"))
//	fmt.Println(et2)
//}

// 测试 tempdir
func TestTempDir(t *testing.T) {
	dir := TempDir()
	fmt.Println(dir)
}

// 测试 filepath.Join
func TestDB_Self(t *testing.T) {
	dirPath := "/tem/cckvt"
	pt := filepath.Join(dirPath, fmt.Sprintf("%09d"+"data", 100))
	fmt.Println(pt) // /tem/cckvt/000000100data
}

// 测试 ReadDir，和 fmt.Sscanf
func TestDB_Self1(t *testing.T) {
	file, err := os.OpenFile("/tmp/cckv100/cckv1.data", os.O_RDWR|os.O_CREATE, 0666)

	file, err = os.OpenFile("/tmp/cckv100/cckv2.data", os.O_RDWR|os.O_CREATE, 0666)

	file, err = os.OpenFile("/tmp/cckv100/cckv3.data", os.O_RDWR|os.O_CREATE, 0666)

	file, err = os.OpenFile("/tmp/cckv100/cckv4.data", os.O_RDWR|os.O_CREATE, 0666)

	dirEntrys, err := os.ReadDir("/tmp/cckv100")
	ids := make([]int, 0)
	for i := 0; i < len(dirEntrys); i++ {
		id := 0
		_, err := fmt.Sscanf(dirEntrys[i].Name(),"cckv%d.data", &id)
		if err != nil {
			return
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)  // 1 2 3 4

	fmt.Println(file, err)
}
