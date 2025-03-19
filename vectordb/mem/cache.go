package mem

import (
	"container/list"
	"github.com/viant/gds/tree/cover"
	"sync"
)

type MRUCache struct {
	maxEntries int
	cache      map[string]*cover.Point
	order      *list.List
	mu         sync.Mutex
}

func NewMRUCache(maxEntries int) *MRUCache {
	return &MRUCache{
		maxEntries: maxEntries,
		cache:      make(map[string]*cover.Point),
		order:      list.New(),
	}
}

func (c *MRUCache) Get(key string) (*cover.Point, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		c.order.MoveToFront(c.order.Back())
		return elem, true
	}
	return nil, false
}

func (c *MRUCache) Put(key string, value *cover.Point) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, found := c.cache[key]; found {
		c.order.MoveToFront(c.order.Back())
		return
	}

	if c.order.Len() >= c.maxEntries {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.cache, oldest.Value.(string))
		}
	}

	c.order.PushFront(key)
	c.cache[key] = value
}
