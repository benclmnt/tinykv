package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	engines := engine_util.NewEngines(engine_util.CreateDB(conf.DBPath, conf.Raft), nil, conf.DBPath, "")
	return &StandAloneStorage{
		Engines: engines,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &reader{
		store: s.Engines.Kv,
		tx:    s.Engines.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		default:
			return fmt.Errorf("unknown modify type: %v", data)
		}
	}
	return s.Engines.WriteKV(wb)

}

type reader struct {
	store     *badger.DB
	tx        *badger.Txn
	iterators []*engine_util.BadgerIterator
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	b, err := engine_util.GetCF(r.store, cf, key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return b, nil
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf, r.tx)
	r.iterators = append(r.iterators, it)
	return it
}

func (r *reader) Close() {
	for _, it := range r.iterators {
		it.Close()
	}
	r.tx.Discard()
}
