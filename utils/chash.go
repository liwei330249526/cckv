package utils

import "hash/fnv"

// 哈希函数
func Mhash(key []byte) uint32 {
	n32 := fnv.New32()
	n32.Write(key)
	ret := n32.Sum32()
	return ret
}
