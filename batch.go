package cckv

import (
	"errors"
	"github.com/liwei330249526/cckv/utils"
	"sync"
)


// 数据结构

const (
	OPDEL = 0
	OPPUT = 1

	PENDING = 0
	COMMITED = 1
	ROLLBACKED = 2
)

type Op struct {
	key []byte
	val []byte
	op int
}

type Batch struct {
	lock sync.RWMutex // 读写锁
	db *DB // 库
	pendingOps map[uint32]*Op // batch的写，删除操作序列; 包括KV
	state int
}

// Put 写, batch 写kv
func (b *Batch) Put(key []byte, value []byte) error {
	// 1 如果 pendingWrite 没有 key 对应的写操作则 append
	opInput := &Op{
		key: key,
		val: value,
		op: OPPUT,
	}

	// 2 如果不存在，添加
	op := b.lookupPendingWrites(key)
	if op == nil {
		b.appendPendingWrites(key, opInput)
	} else {
		// 如果存在，则原地修改
		op.val = value
		op.op = OPPUT
	}
	return nil
}

// Get 读， batch 读kv
func (b *Batch) Get(key []byte) ([]byte, error) {
	// 1 如果 pendingWrite  存在 key 对应的数据， 则返回
	op := b.lookupPendingWrites(key)
	if op != nil {
		if op.op == OPPUT {
			return op.val, nil
		} else {
			return nil, errors.New("key already del")
		}

	}

	// 2 如果不存在，则从 index 索引中查找到key 对应的 pos
	// 3 从数据文件中读取对应的数据， 即从db中读取数据
	ov := b.db.Get(key)
	if ov == nil {
		return nil, errors.New("key not exist")
	}
	return ov.value, nil
}

// Delete 删除
func (b *Batch) Delete(key []byte) error {
	opInput := &Op{
		key: key,
		op: OPDEL,
	}

	// 如果不存在，则 append 一条删除操作
	op := b.lookupPendingWrites(key)
	if op == nil {
		b.appendPendingWrites(key, opInput)
	} else {
		// 如果 pendingWrite 不存在对应的key，则直接在该操作上设置mask 删除标记
		op.op = OPDEL
	}
	return nil
}

// Exist 判断key是否存在
func (b *Batch) Exist(key []byte) (bool, error) {
	//1 如果 pendingWrite  存在，则返回true
	khs := utils.Mhash(key)
	op := b.pendingOps[khs]
	if op != nil {
		return true, nil
	}

	//2 如果 db中存在，且 mask 为put 则返回true
	ov := b.db.Get(key)
	if ov != nil {
		return true, nil
	}
	//3 返回false
	return false, errors.New("key not exist")
}

// Commit 提交 batch， 将batch中的数据写盘
func (b *Batch) Commit() error {
	//1 将 pendingWrite 的所有写操作，写入db， 返回一个数组，包含每个写操作的地址
	//2 将 每个写操作的 key ， 和 写地址写入 index 索引
	for _, v := range b.pendingOps {
		//opInput := &Op{
		//	key: v.key,
		//	val: v.val,
		//	op: v.op,
		//}
		if v.op == OPPUT {
			b.db.Put(v.key, v.val)
		} else if v.op == OPDEL {
			b.db.Del(v.key)
		}
	}

	b.state = COMMITED
	b.Reset()
	return nil
}

// Rollback 回滚，即将 pendingWrite 的写操作清空掉
func (b *Batch) Rollback() error {
	//清空 pendingWrite
	b.pendingOps = make(map[uint32]*Op)
	b.state = ROLLBACKED

	b.Reset()
	return nil
}

func (b *Batch) Reset() error {
	b.pendingOps = make(map[uint32]*Op)
	b.state = PENDING
	return nil
}

//  根据key 查找bach 中的数据
func (b *Batch) lookupPendingWrites(key []byte) *Op {
	khs := utils.Mhash(key)
	// 从 pendingWrite 中查找数据
	return b.pendingOps[khs]
}

// 将操作， 加入到 pendingWrite 中
func (b *Batch) appendPendingWrites(key []byte, op *Op) {
	khs := utils.Mhash(key)
	b.pendingOps[khs] = op
	return
}

// 将操作， 加入到 pendingWrite 中
func (b *Batch) delPendingWrites(key []byte) *Op {
	khs := utils.Mhash(key)
	op := b.pendingOps[khs]
	if op != nil {
		delete(b.pendingOps, khs)
	}

	return op
}
