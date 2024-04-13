package index

type MMap struct {
	index map[string]*PosInfo
}

func NewMMap() *MMap {
	t := &MMap{
		index: make(map[string]*PosInfo),
	}
	return t
}

// Put 问题，这个 []byte, 怎么传入函数 func (t *BTree) Get(key Item) Item
// 答： key，val 组成一个条目， 该条目实现Less 方法，作为一个条目插入到b+树中，b+树种对key排序即可;
// 问题， Cannot use 'kv' (type KeyVal) as the type Item Type does not implement 'Item'
// need the method: Less(than Item) bool have the method: Less(k2 KeyVal) bool
func (t *MMap)Put(key []byte, posInfo *PosInfo) *PosInfo {
	oldInfo := t.index[string(key)]
	t.index[string(key)] = posInfo
	return oldInfo
}

// Get 读， 通过key 获取所以条目
func (t *MMap)Get(key []byte) *PosInfo {
	info := t.index[string(key)]
	return info
}

// Delete 删
func (t *MMap)Delete(key []byte) *PosInfo{
	oldInfo := t.index[string(key)]
	delete(t.index, string(key))
	return oldInfo
}

// Size 大小
func (t *MMap)Size() int {
	size := len(t.index)
	return size
}

// Ascend 正序遍历
func (t *MMap)Ascend(handle func(key []byte, posInfo *PosInfo) error) {
	for k, v := range t.index {
		if err := handle([]byte(k), v); err != nil {
			return
		}
	}
	return
}

// Descend 逆序遍历, 没必要，hash索引本来也无序
func (t *MMap)Descend(handle func(key []byte, posInfo *PosInfo) error) {
	for k, v := range t.index {
		if err := handle([]byte(k), v); err != nil {
			return
		}
	}
	return
}
