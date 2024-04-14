package cckv

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cckvlbs/dep/index"
)

const (
	DataFileSuffix = ".data" // 数据文件后缀
)

// TempDir 获取一个临时目录
func TempDir() string {
	path,_ := os.MkdirTemp("", "cckv_temp")
	return path
}

type DB struct {
	dFile *DbFiles
	index index.Index
	dir string
}

// OpenDb 打开db
func OpenDb(dir string) *DB {
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) { // 如果dir不存在， 则创建dir
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			fmt.Errorf("dir err %s\n", err)
			return nil
		}
	}

	file := OpenFile(dir) // 打开文件
	if file == nil {
		fmt.Errorf("open file err")
		return nil
	}

	db := &DB{ // 返回db句柄
		dFile: file,
		//index: make(map[string]int),
		//index: index.NewMMap(),
		index: index.NewMBTree(),
		dir: dir,
	}
	// 加载索引
	err = db.loadIndex()
	if err != nil {
		return nil
	}

	return db
}
func (db *DB)NewBatch() *Batch {
	b := &Batch{
		lock: sync.RWMutex{},
		db: db,   // 这里什么时候赋值
		pendingOps: make(map[uint32]*Op),
		state:PENDING,
	}
	return b
}
//func NewBatch() *Batch {
//	b := &Batch{
//		lock: sync.RWMutex{},
//		db: OpenDb("/tmp/cckv"),   这里什么时候赋值
//		pendingOps: make(map[uint32]*Op),
//		state:PENDING,
//	}
//	return b
//}

// db建立索引
func (db *DB)loadIndex() error {
	if db.dFile.activeFile.pos == 0 {
		fmt.Errorf("file pos is 0")
		return nil
	}

	// 读取所有数据，根据数据建立索引， 顺序问题，先读old， 再读active，对吗？
	for i := 0; i < len(db.dFile.oldFiles); i++ {
		fr := NewFileRead(db.dFile.oldFiles[i])
		for {
			err, et, psIf := fr.next()
			if err == io.EOF {
				break
			}
			if et.mask == 1 {
				db.index.Put(et.key, psIf)
				//db.index.Put(et.key, &index.PosInfo{Pos: pos})
				//db.index[string(et.key)] = pos
			}
		}
	}

	// 然后再读取active file
	fr := NewFileRead(db.dFile.activeFile)
	for {
		err, et, psIf := fr.next()
		if err == io.EOF {
			break
		}
		if et.mask == 1 {
			db.index.Put(et.key, psIf)
			//db.index.Put(et.key, &index.PosInfo{Pos: pos})
			//db.index[string(et.key)] = pos
		}
	}

	//
	//pos := 0
	//for {
	//	err, et := db.dFile.ReadFile(pos) // 读取所有数据
	//	if err != nil {
	//		if err == io.EOF {
	//			break
	//		} else {
	//			return err
	//		}
	//
	//	}
	//
	//	if et.mask == 1 {
	//		db.index.Put(et.key, &index.PosInfo{Pos: pos})
	//		//db.index[string(et.key)] = pos
	//	}
	//
	//	pos += et.Size()
	//}
	// maybe there add pos to dFile.pos
	return nil
}

// Put db写入
func (db *DB)Put(key []byte, val []byte) {
	et := NewEntry([]byte(key), []byte(val))
	et.mask = OPPUT
	//pos := db.dFile.activeFile.pos // 用于建立索引
	err,posInf := db.dFile.WriteFile(et)
	if err != nil {
		return
	}

	//db.index.Put(key, &index.PosInfo{Pos: pos})
	//db.index[string(key)] = pos // 建立索引
	db.index.Put(key, posInf) // 建立索引
	return
}

// Get db获取
// 问题， db这里怎么知道读结束了？
//  1 通过返回了nil， 或 加一个err 返回值
func (db *DB)Get(key []byte) *Entry {
	if db.index.Get(key) == nil {
		return nil
	}

	posInf := db.index.Get(key)

	_, resEt := db.dFile.ReadFile(posInf)
	if resEt == nil {
		return nil
	}

	return resEt
}

// Del 删除key
func (db *DB)Del(key []byte) {
	et := NewEntry([]byte(key), []byte{})
	et.mask = OPDEL
	//pos := db.dFile.pos // 用于索引
	err, _ := db.dFile.WriteFile(et)
	if err != nil {
		return
	}

	db.index.Delete(key)
	//delete(db.index, string(key))
	//db.index[string(key)] = pos // 建立索引
}
// 日志追加写的，需要定期merge冗余数据
//func (db *DB)Merge() error {
//	// new file
//	path := db.dFile.activeFile.file.Name()
//	patht := path+"t"
//
//	filet := OpenFile(patht) // 打开文件
//	if filet == nil {
//		fmt.Errorf("open file err")
//		return errors.New("open file err")
//	}
//	// read data, trim data
//	// 读取所有数据
//	mem := make([]*Entry, 0)
//	pos := 0
//	for {
//		err, et := db.dFile.ReadFile(pos) // 读取所有数据
//		if err != nil {
//			if err == io.EOF {
//				break
//			} else {
//				return err
//			}
//
//		}
//
//		if et.mask == OPPUT {
//			posInf := db.index.Get(et.key)
//			if posInf == nil {
//				pos += et.Size()
//				continue
//			}
//			if posInf.Pos == pos {
//				mem = append(mem, et)
//			}
//		}
//
//		pos += et.Size()
//	}
//
//	// write new file
//	pos = 0
//	for i := 0; i < len(mem); i++ {
//		err, posInf := filet.WriteFile(mem[i])
//		if err != nil {
//			return err
//		}
//		//db.index.Put(mem[i].key, &index.PosInfo{Pos:pos})
//		//db.index[string(mem[i].key)] = pos
//		db.index.Put(mem[i].key, posInf) // 建立索引
//		pos += mem[i].Size()
//	}
//
//	// remove old file
//	db.dFile.Close()
//	os.Remove(path)
//
//	// rename file
//	os.Rename(patht, path)
//	file := OpenFile(path) // 打开文件
//	if file == nil {
//		fmt.Errorf("open file err")
//		return errors.New("open file err")
//	}
//	db.dFile = file
//
//
//	return nil
//}