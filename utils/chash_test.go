package utils

import (
	"fmt"
	"testing"
)

func TestChash(t *testing.T) {
	key := "li"
	hkey := chash([]byte(key))
	fmt.Println(hkey)
}