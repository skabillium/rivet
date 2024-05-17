package storage

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/c4pt0r/kvql"
	"go.etcd.io/bbolt"
)

var DataBucketName = []byte("data")

type DiskStorageOptions struct {
	File string
}

func NewDiskStorage(opts DiskStorageOptions) (*DiskStorage, error) {
	dir, _ := filepath.Split(opts.File)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	db, err := bbolt.Open(opts.File, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(DataBucketName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &DiskStorage{db: db, bucket: DataBucketName}, nil
}

type DiskStorage struct {
	db     *bbolt.DB
	bucket []byte
}

func (kv *DiskStorage) Keys() []string {
	keys := []string{}
	kv.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		buck.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
		return nil
	})
	return keys
}

func (kv *DiskStorage) Get(key []byte) ([]byte, error) {
	var value []byte
	kv.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		value = buck.Get(key)
		return nil
	})
	return value, nil
}

func (kv *DiskStorage) Put(key []byte, value []byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		return buck.Put(key, value)
	})
}

func (kv *DiskStorage) Delete(key []byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(kv.bucket).Delete(key)
	})
}

func (kv *DiskStorage) BatchPut(kvs []kvql.KVPair) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		var err error
		for _, pair := range kvs {
			err = buck.Put(pair.Key, pair.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (kv *DiskStorage) BatchDelete(keys [][]byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		var err error
		for _, k := range keys {
			err = buck.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (kv *DiskStorage) Cursor() (kvql.Cursor, error) {
	return &DiskStorageCursor{
		bucket: kv.bucket,
		db:     kv.db,
	}, nil
}

func (kv *DiskStorage) Close() {
	kv.db.Close()
}

type DiskStorageCursor struct {
	prefix  []byte
	currKey []byte
	currVal []byte
	bucket  []byte
	db      *bbolt.DB
}

func (c *DiskStorageCursor) Seek(prefix []byte) error {
	c.prefix = prefix
	return c.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(c.bucket).Cursor()
		k, v := cursor.Seek(prefix)
		c.currKey = k
		c.currVal = v
		return nil
	})
}

func (c *DiskStorageCursor) Next() (key []byte, value []byte, err error) {
	var prevKey, prevVal []byte
	c.db.View(func(tx *bbolt.Tx) error {
		prevKey = c.currKey
		prevVal = c.currVal
		cursor := tx.Bucket(c.bucket).Cursor()
		cursor.Seek(prevKey)
		k, v := cursor.Next()
		if bytes.HasPrefix(k, c.prefix) {
			c.currKey, c.currVal = k, v
		} else {
			c.prefix, c.currKey, c.currVal = nil, nil, nil
		}
		return nil
	})
	return prevKey, prevVal, nil
}
