package backend

import (
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
)

type leveldbBackend struct {
	db *leveldb.DB
}

func NewLeveldbBackend(path string) (*leveldbBackend, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &leveldbBackend{db: db}, nil
}

func (l *leveldbBackend) Put(key, value string) error {
	return l.db.Put([]byte(key), []byte(value), nil)
}

func (l *leveldbBackend) Get(key string) (string, error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (l *leveldbBackend) Delete(key string) error {
	return l.db.Delete([]byte(key), nil)
}

func (l *leveldbBackend) GetSnapshot() ([]byte, error) {
	snapshotData := make(map[string]string)
	iter := l.db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())
		snapshotData[key] = value
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return json.Marshal(snapshotData)
}
