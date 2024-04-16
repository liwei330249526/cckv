package cckv

import (
	"errors"
	"fmt"
	"github.com/cckvlbs/dep/index"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

// DbFileElement 维护了文件编号（即 cckv1.data, cckv2.data），即其中的1，2
//一个文件的句柄，和写偏移（即，下次要再这个地方写数据）
type DbFileElement struct {
	fileId  int
	file *os.File
	pos int // write 从这里写
}

func DataPath(dir string, id int) string {
	dataPath := path.Join(dir, fmt.Sprintf("cckv%d.data", id))
	return dataPath
}

func DataMergePath(dir string, id int) string {
	dataPath := filepath.Join(dir,"merge", fmt.Sprintf("cckv%d.data", id))
	return dataPath
}

// WriteFile 写 entry 到文件的 pos
func (d *DbFileElement)WriteFile(entry *Entry) (error, *index.PosInfo) {
	if d.file == nil {
		return errors.New("write file nil"),nil
	}
	buf := EnCode(entry)

	_, err := d.file.WriteAt(buf, int64(d.pos))
	if err != nil {
		return err, nil
	}

	retPosInfo := &index.PosInfo{FileId: d.fileId, Pos: d.pos}
	d.pos += len(buf)

	return nil, retPosInfo
}

// ReadFile 通过 posIf 读取数据 entry
func (d *DbFileElement)ReadFile(pos int) (error, *Entry){
	if d.file == nil {
		fmt.Println("Read file nil")
		return errors.New("Read file nil"), nil
	}
	//pos := posIf.Pos
	headBuf := make([]byte, EntryHeadSize)
	_, err := d.file.ReadAt(headBuf, int64(pos))
	if err != nil {
		fmt.Println("Read file err ", err)
		return err, nil
	}
	pos += EntryHeadSize

	entry := DeCode(headBuf)
	reskey := make([]byte, entry.keySize)
	resval := make([]byte, entry.valSize)

	if entry.keySize != 0 {
		_, err = d.file.ReadAt(reskey, int64(pos))
		if err != nil {
			fmt.Println("Read file err ", err)
			return err, nil
		}
		pos += int(entry.keySize)
		entry.key = reskey
	}

	if entry.valSize != 0 {
		_, err = d.file.ReadAt(resval, int64(pos))
		if err != nil {
			fmt.Println("Read file err ", err)
			return err, nil
		}
		pos += int(entry.valSize)
		entry.value = resval
	}
	return nil, entry
}

func (d *DbFileElement)Close(){
	d.file.Close()
	return
}

// DbFiles , 维护了多个文件的句柄，包括一个active，和多个 old
type DbFiles struct {
	activeFile *DbFileElement
	oldFiles []*DbFileElement
}

// OpenFile 打开一个文件， 如果不存在，则新建
func OpenFile(dir string) *DbFiles {
	// DBFile 对象
	dbFile := &DbFiles{}

	//1 读取dir 的所有文件。 os.ReadDir
	dirEntrys, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	opFile := func (id int) *DbFileElement {
		dataPath := DataPath(dir, id)
		file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0666) // exist err
		if err != nil {
			fmt.Errorf("new file err %s\n", err)
			return nil
		} // how do open a exist file, bug no err
		// add len of file
		fInf, err := os.Stat(dataPath)
		if err != nil{
			fmt.Errorf("Stat file err %s\n", err)
			return nil
		}
		dbf := &DbFileElement{fileId: id, file: file, pos: int(fInf.Size())}
		//dbFile.activeFile = dbf
		return dbf
	}

	//2 如果文件数为0， 则新建 1.data
	if len(dirEntrys) == 0 {
		atDbf := opFile(0)
		if atDbf == nil {
			fmt.Errorf("new file err %s\n", err)
		}
		dbFile.activeFile = atDbf
		//dbFile.pos = int(fInf.Size())
		//dbFile.activeFile.file = file
	} else {
		//3 如果文件数不为0，则例如，原有 1.data,2.data, 3,data; 则1,2为old， 3为active
		//; Sscanf   sort
		ids := make([]int, 0)
		for i := 0; i < len(dirEntrys); i++ {
			id := 0
			_, err := fmt.Sscanf(dirEntrys[i].Name(),"cckv%d.data", &id)
			if err != nil {
				continue
			}
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for i := 0; i < len(ids); i++ {
			var oldDbf *DbFileElement
			oldDbf = opFile(i)
			if oldDbf == nil {
				fmt.Errorf("new file err %s\n", err)
			}
			if i != len(ids)-1 {
				dbFile.oldFiles = append(dbFile.oldFiles, oldDbf)
			} else {
				dbFile.activeFile = oldDbf
			}
		}
	}

	//4 将老的文件句柄放入 oldSet, 最新的文件句柄放入 active指针.


	//dbFile := &DbFiles{}
	//file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0666) // exist err
	//if err != nil {
	//	fmt.Errorf("new file err %s\n", err)
	//	return nil
	//} // how do open a exist file, bug no err
	//// add len of file
	//fInf, err := os.Stat(dataPath)
	//if err != nil{
	//	fmt.Errorf("Stat file err %s\n", err)
	//	return nil
	//}
	//dbFile.pos = int(fInf.Size())
	//dbFile.activeFile.file = file
	return dbFile
}

// 新建一个新的active 文件，应该 dbFile 的逻辑
// 新建一个actviefile，将老的放入oldFiles集合。
func (df *DbFiles)OpenNewActiveFile(dir string) {
	//新建一个actviefile，将老的放入oldFiles集合。
	opFile := func (id int) *DbFileElement {
		dataPath := DataPath(dir, id)
		file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0666) // exist err
		if err != nil {
			fmt.Errorf("new file err %s\n", err)
			return nil
		} // how do open a exist file, bug no err
		// add len of file
		fInf, err := os.Stat(dataPath)
		if err != nil{
			fmt.Errorf("Stat file err %s\n", err)
			return nil
		}
		dbf := &DbFileElement{fileId: id, file: file, pos: int(fInf.Size())}
		//dbFile.activeFile = dbf
		return dbf
	}

	newActive := opFile(df.activeFile.fileId+1)
	df.oldFiles = append(df.oldFiles, df.activeFile)
	df.activeFile = newActive
	return
}

// WriteFile 向文件写一个 entry
func (dbFile *DbFiles) WriteFile(entry *Entry) (error,*index.PosInfo)  {
	if dbFile.activeFile == nil {
		return errors.New("write file nil"),nil
	}
    // 这里写 actvieFile 即可
	return dbFile.activeFile.WriteFile(entry)

	//buf := EnCode(entry)
	//
	//_, err := dbFile.activeFile.file.WriteAt(buf, int64(dbFile.activeFile.pos))
	//if err != nil {
	//	return err, nil
	//}
	//
	//retPosInfo := &index.PosInfo{FileId: dbFile.activeFile.fileId, Pos: dbFile.activeFile.pos}
	//dbFile.activeFile.pos += len(buf)
	//
	//return nil, retPosInfo
}

// ReadFile 从文件读一个entry
func (dbFile *DbFiles) ReadFile(pos *index.PosInfo) (error, *Entry) {
	if dbFile.activeFile.file == nil {
		fmt.Println("Read file nil")
		return errors.New("Read file nil"), nil
	}

	var dbfEle *DbFileElement
	// 如果是active 的文件id，则从actvie的文件读取
	if dbFile.activeFile.fileId == pos.FileId {
		dbfEle = dbFile.activeFile
	} else {
		// 如果不是actvie的文件id，则从old的文件读取
		// fildid校验
		if pos.FileId >= dbFile.oldFiles[len(dbFile.oldFiles)-1].fileId {
			return errors.New("Read file nil"), nil
		}
		dbfEle = dbFile.oldFiles[pos.FileId]
	}

	return dbfEle.ReadFile(pos.Pos)

	//
	//
	//headBuf := make([]byte, EntryHeadSize)
	//_, err := dbFile.activeFile.file.ReadAt(headBuf, int64(pos))
	//if err != nil {
	//	fmt.Println("Read file err ", err)
	//	return err, nil
	//}
	//pos += EntryHeadSize
	//
	//entry := DeCode(headBuf)
	//reskey := make([]byte, entry.keySize)
	//resval := make([]byte, entry.valSize)
	//
	//if entry.keySize != 0 {
	//	_, err = dbFile.activeFile.file.ReadAt(reskey, int64(pos))
	//	if err != nil {
	//		fmt.Println("Read file err ", err)
	//		return err, nil
	//	}
	//	pos += int(entry.keySize)
	//	entry.key = reskey
	//}
	//
	//if entry.valSize != 0 {
	//	_, err = dbFile.activeFile.file.ReadAt(resval, int64(pos))
	//	if err != nil {
	//		fmt.Println("Read file err ", err)
	//		return err, nil
	//	}
	//	pos += int(entry.valSize)
	//	entry.value = resval
	//}
	//return nil, entry
}

// DbFiles 关闭
func (dbFile *DbFiles) Close() {
	for i := 0; i < len(dbFile.oldFiles); i++ {
		of := dbFile.oldFiles[i]
		of.Close()
	}
	dbFile.oldFiles = nil
	dbFile.activeFile.Close()
	return
}

func NewFileRead(dbFile *DbFileElement) *fileReader {
	fr := &fileReader{
		file: dbFile,
		readPos: 0,
	}
	return fr
}

// 一个文件的读取器，包含文件句柄，和读pos， 通过next 函数去遍历文件的所有 entry
type fileReader struct {
	file *DbFileElement // 这个文件
	readPos int // 读取器从这里读取
}
// return , value, posInfo, err
func (fr *fileReader) next() (error, *Entry, *index.PosInfo) {
	//for {
		err, et := fr.file.ReadFile(fr.readPos) // 读取所有数据
		if err != nil {
			return err, nil, nil
			//if err == io.EOF {
			//
			//} else {
			//
			//}
		}
		retPosInfo := &index.PosInfo{Pos: fr.readPos}
		fr.readPos += et.Size()

		return nil, et, retPosInfo
		//if et.mask == 1 {
		//	db.index.Put(et.key, &index.PosInfo{Pos: pos})
		//	//db.index[string(et.key)] = pos
		//}
		//
		//pos += et.Size()
	//}
}

// FileReaders 多个文件的读取器，一次遍历数组中的所有文件读取器，去读取所有文件的entry
type FileReaders struct {
	fileReaders []*fileReader
	id int
}

// NewfileReaders 新建多文件读取器
func NewfileReaders(dbFile *DbFiles) *FileReaders {
	//1 从 DbFile ，获取所有的 DbFileElement，构造对应的 reader， 加入到 fileReaders 中

	fs := &FileReaders{}
	for i := 0; i < len(dbFile.oldFiles); i++ {
		fr := NewFileRead(dbFile.oldFiles[i])
		fs.fileReaders = append(fs.fileReaders, fr)
	}

	fr := NewFileRead(dbFile.activeFile)
	fs.fileReaders = append(fs.fileReaders, fr)

	//2 对 fileReaders 按照 fildId排序，返回
	sort.Slice(fs.fileReaders, func(i, j int) bool {
		return fs.fileReaders[i].file.fileId < fs.fileReaders[i].file.fileId
	})

	return fs
}

func (fs *FileReaders)Next() (error, *Entry, *index.PosInfo) {
	//1 如果readId 超出了最大，则返回 io.EOF
	if fs.id >= len(fs.fileReaders) {
		return io.EOF, nil, nil
	}

	//2 通过readId的 fileReader 读取一个 entry 返回，
	//3 如果 read 返回 io.EOF, 则增大 readerId, 即可
	err, et, psIf := fs.fileReaders[fs.id].next()
	if err == io.EOF {
		fs.id++
		err, et, psIf = fs.Next()
	}
	return err, et, psIf
}

