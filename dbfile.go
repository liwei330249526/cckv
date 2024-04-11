package cckv

import (
	"errors"
	"fmt"
	"os"
)

type DbFile struct {
	file *os.File
	pos int
}

// OpenFile 打开一个文件， 如果不存在，则新建
func OpenFile(path string) *DbFile {
	dbFile := &DbFile{}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666) // exist err
	if err != nil {
		fmt.Errorf("new file err %s\n", err)
		return nil
	} // how do open a exist file, bug no err
	// add len of file
	fInf, err := os.Stat(path)
	if err != nil{
		fmt.Errorf("Stat file err %s\n", err)
		return nil
	}
	dbFile.pos = int(fInf.Size())
	dbFile.file = file
	return dbFile
}

// WriteFile 向文件写一个 entry
func (dbFile *DbFile) WriteFile(entry *Entry) error  {
	if dbFile.file == nil {
		return errors.New("write file nil")
	}
	buf := EnCode(entry)

	_, err := dbFile.file.WriteAt(buf, int64(dbFile.pos))
	if err != nil {
		return err
	}
	dbFile.pos += len(buf)
	return nil
}

// ReadFile 从文件读一个entry
func (dbFile *DbFile) ReadFile(pos int) (error, *Entry) {
	if dbFile.file == nil {
		fmt.Println("Read file nil")
		return errors.New("Read file nil"), nil
	}
	headBuf := make([]byte, EntryHeadSize)
	_, err := dbFile.file.ReadAt(headBuf, int64(pos))
	if err != nil {
		fmt.Println("Read file err ", err)
		return err, nil
	}
	pos += EntryHeadSize

	entry := DeCode(headBuf)
	reskey := make([]byte, entry.keySize)
	resval := make([]byte, entry.valSize)

	if entry.keySize != 0 {
		_, err = dbFile.file.ReadAt(reskey, int64(pos))
		if err != nil {
			fmt.Println("Read file err ", err)
			return err, nil
		}
		pos += int(entry.keySize)
		entry.key = reskey
	}

	if entry.valSize != 0 {
		_, err = dbFile.file.ReadAt(resval, int64(pos))
		if err != nil {
			fmt.Println("Read file err ", err)
			return err, nil
		}
		pos += int(entry.valSize)
		entry.value = resval
	}
	return nil, entry
}

func (dbFile *DbFile) Close() {
	dbFile.file.Close()
	return
}
