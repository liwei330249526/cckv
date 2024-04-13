package index

type Index interface{
	Put(key []byte, posInfo *PosInfo) *PosInfo                      // 写, key 和索引条目
	Get(key []byte) *PosInfo                                        // 读， 通过key 获取所以条目
	Delete(key []byte) *PosInfo                                 // 删
	Size() int                                                      // 大小
	Ascend(handle func(key []byte, posInfo *PosInfo)error)  // 正序遍历
	Descend(handle func(key []byte, posInfo *PosInfo)error) // 逆序遍历
}