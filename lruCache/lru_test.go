package lruCache

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewLruCache(64)
	if cache == nil {
		t.Errorf("cache is nil")

	}
}

func TestLruCache_Get(t *testing.T) {
	lru := NewLruCache(64)
	lru.Set([]byte("k-1"), []byte("ss"))
	lru.Set([]byte("k-1"), []byte("dd"))
	lru.Set([]byte("k-1"), []byte("ssss"))

	lru.Set([]byte("k-2"), []byte("bbb"))

	v, ok := lru.Get([]byte("k-1"))
	t.Log(string(v), ok) // expect ssss

	v1, ok := lru.Get([]byte("k-2")) // expect bbb
	t.Log(string(v1), ok)
}

func TestLruCache_Set(t *testing.T) {
	lru := NewLruCache(100)
	lru.Set(nil, nil)
}

func TestLruCache_Remove(t *testing.T) {
	lru := NewLruCache(10)
	lru.Remove(nil)

	t.Run("1", func(t *testing.T) {
		lru.Set([]byte("1"), []byte("1"))
		lru.Remove([]byte("1"))

		if lru.cacheList.Len()!= 0 {
			t.Errorf("lru.cacheList.Len expect 0")
		}

		if len(lru.cacheMap) != 0 {
			t.Errorf("len(lru.cacheMap) expect 0")
		}
	})

	t.Run("2", func(t *testing.T) {
		lru.Set([]byte("22"), []byte("aa"))
		lru.Set([]byte("2233"), []byte("aa"))

		lru.Remove([]byte("100"))
		if lru.cacheList.Len() != 2 && len(lru.cacheMap) != 2 {
			t.Errorf("cacheList len expect 2, and cacheMap len expect 2")
		}

		lru.Remove([]byte("22"))
		if lru.cacheList.Len() != 1 && len(lru.cacheMap) != 1 {
			t.Errorf("cacheList len expect 1, and cacheMap len expect 1")
		}
	})

	t.Run("3", func(t *testing.T) {
		lru.Set([]byte("33"), []byte("aa"))
		lru.Set([]byte("3344"), []byte("aa"))
		lru.Remove([]byte("33"))
		lru.Remove([]byte("3344"))
	})
}

func BenchmarkLruCache_Set(b *testing.B) {
	cache := NewLruCache(500)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Set(GetKey(i), GetValue())
	}
}

func BenchmarkLruCache_Get(b *testing.B) {
	cache := NewLruCache(1024)
	for i := 0; i < 10000; i++ {
		cache.Set(GetKey(i), GetValue())
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(GetKey(i))
	}
}

func BenchmarkLruCache_Remove(b *testing.B) {
	cache := NewLruCache(1024)
	for i := 0; i < 10000; i++ {
		cache.Set(GetKey(i), GetValue())
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Remove(GetKey(i))
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz"

func GetKey(n int) []byte {
	return []byte("test_key_" + fmt.Sprintf("%09d", n))
}

func GetValue() []byte {
	var str bytes.Buffer
	for i := 0; i < 12; i++ {
		str.WriteByte(alphabet[rand.Int()%26])
	}
	// 前缀， 时间转为10进制int， 12个无序字符
	ret := []byte("test_val-" + strconv.FormatInt(time.Now().UnixNano(), 10) + str.String())
	strR := string(ret)
	return []byte(strR)
}
