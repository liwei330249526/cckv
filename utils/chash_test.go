package utils

import (
	"fmt"
	"testing"
)

func TestChash(t *testing.T) {
	key := "li"
	hkey := Mhash([]byte(key))
	fmt.Println(hkey)
}