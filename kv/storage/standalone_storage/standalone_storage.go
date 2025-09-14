package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	path string
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		path: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.path
	opts.ValueDir = s.path

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// Reader shoul dsupport GET and SCAN operations
// Use badger.Txn so that the transaction handler provided by the badger could provide a consistent snapshot of the keys and values
// badger by default does not support column families. Instead, we should use engine_util to support column families
// Don't forget to call Discard() for badger.Txn and close all iterators before discardin

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	reader := StandAloneStorageReader{s}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := engine_util.WriteBatch{}
	for _, m := range batch {
		switch d := m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(d.Cf, d.Key, d.Value)
		case storage.Delete:
			writeBatch.DeleteCF(d.Cf, d.Key)
		}
	}

	return writeBatch.WriteToDB(s.db)
}

type StandAloneStorageReader struct {
	inner *StandAloneStorage
}

func (sasr StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sasr.inner.db, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, storage.ErrNotFound
	}

	// if val == nil {
	//	return nil, storage.ErrNotFound
	// }

	return val, nil
}

func (sasr StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := sasr.inner.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (sasr StandAloneStorageReader) Close() {
}
