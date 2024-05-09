package cckv

import (
	"fmt"
	"testing"
)

func TestBatch(t *testing.T) {
	db := OpenDb("/tmp/cckv", CecheSizeDefalut) // 新建db
	if db == nil {
		return
	}
	batch := db.NewBatch() // 新建batch

	batch.Put([]byte("li"), []byte("wei")) // batch 中加入操作
	batch.Put([]byte("li2"), []byte("wei2"))

	v10,_ := batch.Get([]byte("li")) // 还未提交，查询可以查到
	v20,_ := batch.Get([]byte("li2"))
	v10str := string(v10)
	v20str := string(v20)
	fmt.Println(v10str, v20str)

	batch.Commit() // 提交， 写盘

	v11,_ := batch.Get([]byte("li")) // 提交后可以查到
	v21,_ := batch.Get([]byte("li2"))
	v11str := string(v11)
	v21str := string(v21)
	fmt.Println(v11str, v21str)

	batch.Delete([]byte("li")) // batch 删除
	batch.Delete([]byte("li2"))
	v12,_ := batch.Get([]byte("li"))
	v22,_ := batch.Get([]byte("li2"))
	v12str := string(v12)
	v22str := string(v22)
	fmt.Println(v12str, v22str) // 不能查到，但盘中还存在


	batch.Commit() // 提交罗盘
	v13,_ := batch.Get([]byte("li"))
	v23,_ := batch.Get([]byte("li2"))
	v13str := string(v13)
	v23str := string(v23)

	fmt.Println(v13str, v23str) // 已删除，不能查到
}

func TestBatch_Rollback(t *testing.T) {
	db := OpenDb("/tmp/cckv", CecheSizeDefalut) // 新建db
	if db == nil {
		return
	}
	batch := db.NewBatch() // 新建batch

	batch.Put([]byte("li"), []byte("wei")) // batch 中加入操作
	batch.Put([]byte("li2"), []byte("wei2"))

	v10,_ := batch.Get([]byte("li")) // 还未提交，查询可以查到
	v20,_ := batch.Get([]byte("li2"))
	v10str := string(v10)
	v20str := string(v20)
	fmt.Println(v10str, v20str)

	batch.Rollback()
	v11,_ := batch.Get([]byte("li")) // 提交后可以查到
	v21,_ := batch.Get([]byte("li2"))
	v11str := string(v11)
	v21str := string(v21)
	fmt.Println(v11str, v21str)
}