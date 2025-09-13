package standalone_storage

import (
	"log"

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
	db *badger.DB
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
	// Your Code Here (1).

	writeBatch := engine_util.WriteBatch{}
	for i := 0; i < len(batch); i++ {
		b := batch[i]
		writeBatch.SetCF(b.Cf(), b.Key(), b.Value())
	}

	err := writeBatch.WriteToDB(s.db)
	if err != nil {
		log.Fatal("Failed to Write: ", err)
		return nil
	}

	return nil
}

type StandAloneStorageReader struct {
	inner *StandAloneStorage
}

func (sasr StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(sasr.inner.db, cf, key)
}

func (sasr StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := sasr.inner.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (sasr StandAloneStorageReader) Close() {
}
