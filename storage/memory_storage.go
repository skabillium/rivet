package storage

import (
	"sort"
	"strings"

	"github.com/c4pt0r/kvql"
)

type MemoryStorage struct {
	data map[string][]byte
	// should use a sorted map or tree to maintain order
	// but for simplicity, we use a slice to maintain order
	orderedKeys []string
}

type MemoryCursor struct {
	keys  []string
	index int
	data  map[string][]byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data:        make(map[string][]byte),
		orderedKeys: make([]string, 0),
	}
}

func (m *MemoryStorage) Keys() []string {
	return m.orderedKeys
}

func (m *MemoryStorage) Get(key []byte) (value []byte, err error) {
	value, ok := m.data[string(key)]
	if !ok {
		return nil, nil // Return nil if the key does not exist
	}
	return value, nil
}

func (m *MemoryStorage) Put(key []byte, value []byte) error {
	strKey := string(key)
	if _, exists := m.data[strKey]; !exists {
		m.orderedKeys = append(m.orderedKeys, strKey)
		sort.Strings(m.orderedKeys) // Maintain order after insertion
	}
	m.data[strKey] = value
	return nil
}

func (m *MemoryStorage) Delete(key []byte) error {
	strKey := string(key)
	if _, exists := m.data[strKey]; exists {
		delete(m.data, strKey)
		i := sort.SearchStrings(m.orderedKeys, strKey)
		m.orderedKeys = append(m.orderedKeys[:i], m.orderedKeys[i+1:]...)
	}
	return nil
}

func (m *MemoryStorage) BatchPut(kvs []kvql.KVPair) error {
	for _, kv := range kvs {
		m.Put(kv.Key, kv.Value)
	}
	return nil
}

func (m *MemoryStorage) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		m.Delete(key)
	}
	return nil
}

func (m *MemoryStorage) Cursor() (cursor kvql.Cursor, err error) {
	return &MemoryCursor{data: m.data, keys: m.orderedKeys, index: -1}, nil
}

func (m *MemoryStorage) Close() {
	m.data = nil
	m.orderedKeys = nil
}

func (c *MemoryCursor) Seek(prefix []byte) error {
	c.index = sort.SearchStrings(c.keys, string(prefix))
	if c.index < len(c.keys) && strings.HasPrefix(c.keys[c.index], string(prefix)) {
		return nil
	}
	c.index = len(c.keys)
	return nil
}

func (c *MemoryCursor) Next() (key []byte, value []byte, err error) {
	if c.index < 0 || c.index >= len(c.keys) {
		return nil, nil, nil
	}
	keyStr := c.keys[c.index]
	value = c.data[keyStr]
	c.index++
	return []byte(keyStr), value, nil
}

func (c *MemoryCursor) Close() error {
	// No resources to release in this simple cursor
	return nil
}
