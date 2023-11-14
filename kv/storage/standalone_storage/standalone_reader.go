package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	txn *badger.Txn
	db  *badger.DB
}

func NewStandAloneReader(txn *badger.Txn, db *badger.DB) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
		db:  db,
	}
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sr.db, cf, key)
	// key不存在时返回nil
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, nil
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneReader) Close() {
	sr.txn.Discard()
}
