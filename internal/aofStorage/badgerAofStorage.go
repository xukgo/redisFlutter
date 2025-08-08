package aofStorage

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"log/slog"
	"os"
	"sync"
)

type BadgerAofStorage struct {
	locker *sync.RWMutex
	dbPath string
	db     *badger.DB
}

func NewBadgerAofStorage(dbPath string) (*BadgerAofStorage, error) {
	opts := badger.DefaultOptions(dbPath)

	opts.ValueThreshold = 32 << 10  // 32KB threshold value （optimize value size mix scene）
	opts.ValueLogFileSize = 1 << 30 // 1GB value log文件
	opts.Compression = options.ZSTD //
	opts.ZSTDCompressionLevel = 1
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	c := &BadgerAofStorage{
		dbPath: dbPath,
		db:     db,
		locker: new(sync.RWMutex),
	}
	return c, nil
}

func (c *BadgerAofStorage) Append(key []byte, value []byte) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	slog.Debug("BadgerAofStorage append", slog.String("key", string(key)), slog.Int("val_len", len(value)))
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (c *BadgerAofStorage) Delete(key []byte) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (c *BadgerAofStorage) DeleteArray(keys [][]byte) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *BadgerAofStorage) Read(key []byte) ([]byte, error) {
	c.locker.RLock()
	defer c.locker.RUnlock()

	var valCopy []byte
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
	})
	return valCopy, err
}

func (c *BadgerAofStorage) ReadFunc(iterator DbWriteIterator) error {
	c.locker.RLock()
	defer c.locker.RUnlock()

	var writeCount int64 = 0
	err := c.db.View(func(txn *badger.Txn) error {
		for {
			if writeCount >= int64(iterator.GetBufferCap()) {
				return nil
			}

			cindex := iterator.GetCurrentKeyIndex()
			key := iterator.NextKey()
			item, err := txn.Get(key)
			if err != nil {
				iterator.SetStartKey(cindex)
				if errors.Is(err, badger.ErrKeyNotFound) {
					return nil
				}
				return err
			}

			if item.ValueSize() > int64(iterator.GetBufferCap()) {
				slog.Warn("get max size data from BadgerDb", slog.Int64("size", item.ValueSize())) //, slog.String("key", stringUtil.NoCopyBytes2String(item.Key())))
				if writeCount > 0 {
					iterator.SetStartKey(cindex)
					return nil
				}
			} else if writeCount+item.ValueSize() > int64(iterator.GetBufferCap()) {
				iterator.SetStartKey(cindex)
				return nil
			}
			err = item.Value(func(val []byte) error {
				wcnt, err := iterator.GetWriter().Write(val)
				writeCount += int64(wcnt)
				iterator.CommitKey(key)
				return err
			})
			if err != nil {
				iterator.SetStartKey(cindex)
				return err
			}
		}
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *BadgerAofStorage) Destroy() {
	c.db.Close()
	os.RemoveAll(c.dbPath)
}
