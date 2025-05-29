package backend

import (
	"encoding/json"
	"errors"
)

type memoryBackend struct {
	db map[string]string
}

func NewMemoryBackend() *memoryBackend {
	return &memoryBackend{db: make(map[string]string)}
}

func (m *memoryBackend) Put(key, value string) error {
	m.db[key] = value
	return nil
}

func (m *memoryBackend) Get(key string) (string, error) {
	v, ok := m.db[key]
	if ok {
		return v, nil
	} else {
		return v, errors.New("failed to get key")
	}
}

func (m *memoryBackend) Delete(key string) error {
	delete(m.db, key)
	return nil
}

func (m *memoryBackend) GetSnapshot() ([]byte, error) {
	return json.Marshal(m.db)
}
