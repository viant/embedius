package mcp

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"
)

type embedCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

type embedEntry struct {
	key string
	vec []float32
}

func newEmbedCache(capacity int) *embedCache {
	if capacity <= 0 {
		return nil
	}
	return &embedCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element, capacity),
	}
}

func (c *embedCache) Get(key string) ([]float32, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		entry := el.Value.(*embedEntry)
		return cloneVec(entry.vec), true
	}
	return nil, false
}

func (c *embedCache) Add(key string, vec []float32) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		entry := el.Value.(*embedEntry)
		entry.vec = cloneVec(vec)
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&embedEntry{key: key, vec: cloneVec(vec)})
	c.items[key] = el
	if c.ll.Len() > c.cap {
		back := c.ll.Back()
		if back != nil {
			c.ll.Remove(back)
			entry := back.Value.(*embedEntry)
			delete(c.items, entry.key)
		}
	}
}

func cloneVec(vec []float32) []float32 {
	if len(vec) == 0 {
		return nil
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	return out
}

func (h *Handler) queryEmbedding(ctx context.Context, query string, model string) ([]float32, bool, error) {
	if h == nil || h.embedder == nil {
		return nil, false, fmt.Errorf("mcp: embedder unavailable")
	}
	key := strings.TrimSpace(query)
	if key == "" {
		return nil, false, fmt.Errorf("mcp: missing query")
	}
	if model != "" {
		key = model + "\n" + key
	}
	if h.embedCache != nil {
		if vec, ok := h.embedCache.Get(key); ok {
			return vec, true, nil
		}
	}
	vecs, err := h.embedder.EmbedDocuments(ctx, []string{query})
	if err != nil {
		return nil, false, err
	}
	if len(vecs) != 1 {
		return nil, false, fmt.Errorf("embedder returned %d vectors for 1 query", len(vecs))
	}
	if h.embedCache != nil {
		h.embedCache.Add(key, vecs[0])
	}
	return cloneVec(vecs[0]), false, nil
}
