package storage

import (
	"github.com/c4pt0r/kvql"
)

type Storage interface {
	Keys() []string
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	BatchPut(kvs []kvql.KVPair) error
	BatchDelete(keys [][]byte) error
	Cursor() (kvql.Cursor, error)
	Close()
}

type InitStorageOptions struct {
	Disk *DiskStorageOptions
}

func StorageInit(opts InitStorageOptions) (Storage, error) {
	if opts.Disk != nil {
		return NewDiskStorage(*opts.Disk)
	}
	return NewMemoryStorage(), nil
}
