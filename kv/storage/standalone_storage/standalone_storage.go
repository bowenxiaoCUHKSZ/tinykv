package standalone_storage

import (
	// "errors"

	"github.com/pingcap-incubator/tinykv/kv/config"
	storage "github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	badger "github.com/Connor1996/badger"

	// "log"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines

	

}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	KvPath := "/tmp/kv"
	KvEngine := engine_util.CreateDB(KvPath, conf)
	storage := &StandAloneStorage{engine: engine_util.NewEngines(KvEngine,nil, KvPath, "")}

	return storage
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	StorageReaderInstance := &StorageReader{
		db : s.engine.Kv,
	}

	return StorageReaderInstance, nil
}


func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.engine.Kv, data.Cf, data.Key, data.Value)
			if err != nil{
				return err
			}
		case storage.Delete:
			err = engine_util.DeleteCF(s.engine.Kv, data.Cf, data.Key)
			if err != nil{
				return err
			}
		}
	}
	return err
}

type StorageReader struct {
	db *badger.DB
}

func (s *StorageReader) GetCF(cf string, key []byte) ([]byte, error){
	// iter through cf and check key
	val, err := engine_util.GetCF(s.db, cf, key)
	if err != nil{
		return nil, err
	}

	return val, err
	
}


func (s *StorageReader) IterCF(cf string) engine_util.DBIterator{
	txn := s.db.NewTransaction(false)


	iter := engine_util.NewCFIterator(cf, txn)
	iter.Rewind()

	return iter
}


func (s *StorageReader) Close(){
	
}
