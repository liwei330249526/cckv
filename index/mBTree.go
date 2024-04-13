package index

import (
	"bytes"
	"github.com/google/btree"
	"sync"
)
// 索引条目结构
type PosInfo struct {
	Pos int
}

type KeyVal struct {
	key []byte
	val PosInfo
}

// Less key, val 要作为一个条目插入到b+树种
func (k *KeyVal)Less(k2 btree.Item) bool {
	return bytes.Compare(k.key,k2.(*KeyVal).key) < 0
}

//func (a Int) Less(b Item) bool {
//	return a < b.(Int)
//}


type MBTree struct {
	lock *sync.RWMutex
	bt *btree.BTree
}

func NewMBTree() *MBTree {
	t := &MBTree{
		lock: &sync.RWMutex{},
		bt: btree.New(32),
	}
	return t
}

// Put 问题，这个 []byte, 怎么传入函数 func (t *BTree) Get(key Item) Item
// 答： key，val 组成一个条目， 该条目实现Less 方法，作为一个条目插入到b+树中，b+树种对key排序即可;
// 问题， Cannot use 'kv' (type KeyVal) as the type Item Type does not implement 'Item'
// need the method: Less(than Item) bool have the method: Less(k2 KeyVal) bool
func (t *MBTree)Put(key []byte, posInfo *PosInfo) *PosInfo {
	kv := &KeyVal{ // 指针实现了Less 方法，所以kv传入指针，才能适配interface接口
		key:key,
		val:*posInfo,
	}
	oldKv := t.bt.ReplaceOrInsert(kv) // 插入的都是指针
	if oldKv == nil {
		return nil
	}
	oldKvv := &(oldKv.(*KeyVal).val)
	return oldKvv
}

// Get 读， 通过key 获取所以条目
func (t *MBTree)Get(key []byte) *PosInfo {
	k := &KeyVal{ // 指针实现了Less 方法，所以kv传入指针，才能适配interface接口
		key:key,
		//val:*posInfo,
	}
	kvv := t.bt.Get(k)
	if kvv == nil {
		return nil
	}
	kv := &(kvv.(*KeyVal).val)
	return kv
}

// Delete 删
func (t *MBTree)Delete(key []byte) *PosInfo {
	k := &KeyVal{ // 指针实现了Less 方法，所以kv传入指针，才能适配interface接口
		key:key,
		//val:*posInfo,
	}
	oldKvv := t.bt.Delete(k)
	if oldKvv == nil {
		return nil
	}
	oldKv := &(oldKvv.(*KeyVal).val)
	return oldKv
}

// Size 大小
func (t *MBTree)Size() int {
	size := t.bt.Len()
	return size
}

// Ascend 正序遍历
func (t *MBTree)Ascend(handle func(key []byte, posInfo *PosInfo) error) {
	t.bt.Ascend(func(i btree.Item) bool {
		if i == nil {
			return false
		}
		k := i.(*KeyVal).key
		v := i.(*KeyVal).val
		if err := handle(k,&v); err != nil {
			return false
		}
		return true
	})
}

// Descend 逆序遍历
func (t *MBTree)Descend(handle func(key []byte, posInfo *PosInfo) error) {
	t.bt.Descend(func(i btree.Item) bool {
		if i == nil {
			return false
		}
		k := i.(*KeyVal).key
		v := i.(*KeyVal).val
		if err := handle(k,&v); err != nil {
			return false
		}
		return true
	})
}

