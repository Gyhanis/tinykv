package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	switch err {
	case badger.ErrKeyNotFound:
		return nil, nil
	case nil:
		return value, nil
	default:
		return nil, err
	}
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
