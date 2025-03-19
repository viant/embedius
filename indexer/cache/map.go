package cache

import (
	"encoding/json"
	"sync"
)

// Map provides a type-safe in-memory key-value store
type Map[K comparable, V any] struct {
	data map[K]*V
	sync.RWMutex
}

// NewMap creates a new type-safe cache instance
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		data: make(map[K]*V),
	}
}

// Get retrieves a value by key, with existence check
func (c *Map[K, V]) Get(key K) (*V, bool) {
	c.RLock()
	defer c.RUnlock()
	v, ok := c.data[key]
	return v, ok
}

// Set stores a value with the given key
func (c *Map[K, V]) Set(key K, value *V) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

// Delete removes a key-value pair
func (c *Map[K, V]) Delete(key K) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, key)
}

// Data returns all cache data as JSON
func (c *Map[K, V]) Data() ([]byte, error) {
	c.RLock()
	defer c.RUnlock()
	return json.Marshal(c.data)
}

// Load populates cache from JSON data
func (c *Map[K, V]) Load(data []byte) error {
	return json.Unmarshal(data, &c.data)
}

// Keys returns all keys in the cache
func (c *Map[K, V]) Keys() []K {
	c.RLock()
	defer c.RUnlock()
	keys := make([]K, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

// Has checks if a key exists in the cache
func (c *Map[K, V]) Has(key K) bool {
	c.RLock()
	defer c.RUnlock()
	_, ok := c.data[key]
	return ok
}

// Size returns the number of items in the cache
func (c *Map[K, V]) Size() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.data)
}

// Clear empties the cache
func (c *Map[K, V]) Clear() {
	c.Lock()
	defer c.Unlock()
	c.data = make(map[K]*V)
}
