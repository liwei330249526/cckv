package lruCache

import (
	"container/list"
	"sync"
)

// LruCache stands for a least recently used cache.
type LruCache struct {
	capacity  int                      // 容量
	cacheMap  map[string]*list.Element // map
	cacheList *list.List               // 链表
	mu        sync.Mutex
}

type lruItem struct {
	key   string // key
	value []byte // val
}

// NewLruCache create a new LRU cache.
func NewLruCache(capacity int) *LruCache {
	lru := &LruCache{}
	if capacity > 0 {
		lru.capacity = capacity
		lru.cacheMap = make(map[string]*list.Element)
		lru.cacheList = list.New()
	}
	return lru
}

// Get ...
func (c *LruCache) Get(key []byte) ([]byte, bool) {
	if c.capacity <= 0 || len(c.cacheMap) <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ele, ok := c.cacheMap[string(key)] // 如果cache中存在
	if ok {
		c.cacheList.MoveToFront(ele) // 则将元素节点移动链表头部
		item := ele.Value.(*lruItem) // 获取 Value， 返回Item 的值
		return item.value, true
	}
	return nil, false                // 如果不存在，返回nil
}

// Set ...
func (c *LruCache) Set(key, value []byte) {
	if c.capacity <= 0 || key == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ele, ok := c.cacheMap[string(key)]
	// 如果cache 中存在，则设置新 map cahce 的该元素的的 value， 并将元素节点移动到链表头部
	if ok {
		item := ele.Value.(*lruItem)
		item.value = value
		c.cacheList.MoveToFront(ele)
	} else {
		// 如果不能存在，则新建元素节点，加入到链表头部， 并更新 map cache
		ele = c.cacheList.PushFront(&lruItem{key: string(key), value: value})
		c.cacheMap[string(key)] = ele

		// 如果超出了容量， 则链表尾部删除元素节点
		if c.cacheList.Len() > c.capacity {
			c.removeOldest()
		}
	}
}

// Remove ...
func (c *LruCache) Remove(key []byte) {
	if c.cacheMap == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.cacheMap[string(key)]; ok {
		delete(c.cacheMap, string(key))
		c.cacheList.Remove(ele)
	}
}

func (c *LruCache) removeOldest() {
	ele := c.cacheList.Back()
	c.cacheList.Remove(ele)
	item := ele.Value.(*lruItem)
	delete(c.cacheMap, item.key)
}
