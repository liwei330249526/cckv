package cckv

import (
	"errors"
	"fmt"
	"github.com/liwei330249526/cckv/lruCache"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/liwei330249526/cckv/index"
)

const (
	DataFileSuffix = ".data" // 数据文件后缀

	CecheSizeDefalut = 500
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
	cache *lruCache.LruCache
}

// OpenDb 打开db
func OpenDb(dir string, cacheSize int) *DB {
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) { // 如果dir不存在， 则创建dir
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			fmt.Errorf("dir err %s\n", err)
			return nil
		}
	}

	file := OpenFiles(dir) // 打开文件
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
		cache : lruCache.NewLruCache(cacheSize),
	}
	// 加载索引
	err = db.loadIndex()
	if err != nil {
		return nil
	}

	return db
}

func (db *DB)DropDB() {
	db.Close()
	os.RemoveAll(db.dir)
	return
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
func (db *DB)loadIndex_version1() error {
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

// 利用 FileReaders 迭代器重建所有数据的索引
func (db *DB)loadIndex() error {
	if db.dFile.activeFile.pos == 0 {
		fmt.Errorf("file pos is 0")
		return nil
	}
	// 读取所有数据，根据数据建立索引， 顺序问题，先读old， 再读active，对吗？
	reader := NewfileReaders(db.dFile)
	for {
		err, et, psIf := reader.Next()
		if err != nil {
			if err == io.EOF {
				break // 不会返回错误
			}
			return err
		}

		if et.mask == 1 {
			db.index.Put(et.key, psIf)
		}
	}
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
	db.cache.Set(key, val)
	return
}

// Get db获取
// 问题， db这里怎么知道读结束了？
//  1 通过返回了nil， 或 加一个err 返回值
func (db *DB)Get(key []byte) []byte {
	if val, ok := db.cache.Get(key); ok {
		return val
	}

	if db.index.Get(key) == nil {
		return nil
	}

	posInf := db.index.Get(key)

	_, resEt := db.dFile.ReadFile(posInf)
	if resEt == nil {
		return nil
	}

	return resEt.value
}


// 对key 追加数据
func (db *DB)Append(key []byte, val []byte) {
	v := db.Get(key)
	if v == nil {
		fmt.Println("Append err, get err")
		return
	}
	newVal := append(v, val...)

	db.Put(key, newVal)
	db.cache.Set(key, val)
	return
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
	db.cache.Remove(key)
	//delete(db.index, string(key))
	//db.index[string(key)] = pos // 建立索引
}
//
////日志追加写的，需要定期merge冗余数据
//func (db *DB)Merge_version1() error {
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

// Merge 日志追加写的，需要定期merge冗余数据
func (db *DB)Merge() error {
	//1 记录activeFileId，用于后续处理
	//preFileId := db.dFile.activeFile.fileId

	//2 新建一个activeFile， actvieFile 要syn后，新建一个actviefile，将老的放入oldFiles集合。
	db.dFile.activeFile.file.Sync()
	db.dFile.OpenNewActiveFile(db.dir)

	//3 打开一个 mergeDb
	mergeDb := OpenDb(filepath.Join(db.dir, "merge"), CecheSizeDefalut)

	//3 通过fileReaders 获取old 库的所有数据，写入到mergeDb
	// 读取所有数据
	reader := NewfileReaders(db.dFile)
	for {
		err, et, psIf := reader.Next()
		if err != nil {
			if err == io.EOF {
				break // 不会返回错误
			}
			return err
		}
		// 如果是 put操作
		if et.mask == OPPUT {
			// 获取old库的索引
			posInfInIndex := db.index.Get(et.key)
			if posInfInIndex == nil {
				continue
			}
			// 如果数据是最新的数据,则写入mergeDb，且写索引
			if psIf.FileId == posInfInIndex.FileId &&
				psIf.Pos == posInfInIndex.Pos {
				err, _ := mergeDb.dFile.WriteFile(et)
				if err != nil {
					return err
				}
				// 无需建立索引，mergeDb 这里只是为了操作merge file
				//mergeDb.index.Put(et.key, posInf) // 建立索引
			}
		}
	}

	//4 打开 merge 的mergeDbfiles, mergeDbfiles 写入一个 mergefinished 标记（t
	//odo） mergeDbfiles 关闭（todo）

	//5 加载 mergeDbfiles , 将所有merge dir 目录下的文件copy 到db 工作目录下
	//5 todo：先关闭old 的所有文件，删除old 目录下的所有文件

	//os.ReadDir()
	fileNames, _ := ioutil.ReadDir(db.dir)
	for _, fileName := range fileNames {
		if !strings.HasSuffix(fileName.Name(), ".data") {
			continue
		}
		os.Remove(filepath.Join(db.dir, fileName.Name()))
	}

	// 移动文件
	moveFile := func(src string, dest string) error {
		err := os.Rename(src, dest)
		return err
	}

	for i := 0; i < len(mergeDb.dFile.oldFiles); i++ {
		//path.Join(dir, fmt.Sprintf("cckv%d.data", id))
		src := DataMergePath(db.dir, i)
		//src := filepath.Join(db.dir,"merge", fmt.Sprintf("cckv%d.data", i))
		_, err := os.Stat(src)
		if err != nil {
			return errors.New(fmt.Sprintf("Merge state err %s\n", err))
		}

		dst := filepath.Join(db.dir, fmt.Sprintf("cckv%d.data", i))
		moveFile(src, dst)
	}
	db.Close()
	src := DataMergePath(db.dir, mergeDb.dFile.activeFile.fileId)
	dst := DataPath(db.dir, mergeDb.dFile.activeFile.fileId)
	moveFile(src, dst)

	//重新打开   mergeDbfiles

	file := OpenFiles(db.dir) // 打开文件
	if file == nil {
		fmt.Errorf("open file err")
		return nil
	}

	db.dFile = file
	db.index = index.NewMBTree()

	// 加载索引
	err := db.loadIndex()
	if err != nil {
		return err
	}
	return nil
	//重新新建索引
	//重新加载索引
}

func (db *DB) Close() {
	db.dFile.Close()
	return
}