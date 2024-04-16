package cckv

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

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
	//os.MkdirAll("/tmp/cckv100",0666)
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

// 更新文件名
func TestRename(t *testing.T) {
	pt1 := filepath.Join("/tmp/cckv100", fmt.Sprintf("%09d"+"data", 100))
	pt2 := filepath.Join("/tmp/cckv101", fmt.Sprintf("%09d"+"data", 101))
	f1, _ := os.OpenFile(pt1, os.O_RDWR|os.O_CREATE, 0666)
	f2, _ := os.OpenFile(pt2, os.O_RDWR|os.O_CREATE, 0666)
	f1.WriteString("111111111111111")
	f2.WriteString("22222")
	os.Rename(pt1, pt2) // 将文件从cckv100.data 改为 cckv101.data
}

func TestSelfReaddir(t *testing.T) {
	r1, _ := ioutil.ReadDir("/tmp")
	r2, _ := os.ReadDir("/tmp")
	fmt.Println(r1,r2)
}

