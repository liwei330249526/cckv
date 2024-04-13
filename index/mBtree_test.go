package index

import (
	"fmt"
	"testing"
)

func TestMBTree(t *testing.T) {
	bt := NewMBTree()
	k1 := string("cckv1")
	v1 := PosInfo{1}

	k2 := string("cckv2")
	v2 := PosInfo{2}

	k3 := string("cckv3")
	v3 := PosInfo{3}
	bt.Put([]byte(k1), &v1)
	bt.Put([]byte(k2), &v2)
	bt.Put([]byte(k3), &v3)

	resv1 := bt.Get([]byte(k1))
	resv2 := bt.Get([]byte(k2))
	resv3 := bt.Get([]byte(k3))
	fmt.Println(resv1, resv2, resv3)
}