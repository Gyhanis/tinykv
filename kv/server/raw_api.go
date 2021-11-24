package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	response := new(kvrpcpb.RawGetResponse)
	response.Reset()
	response.Value = value
	response.NotFound = len(value) == 0
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	mod := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	server.storage.Write(nil, mod)
	response := new(kvrpcpb.RawPutResponse)
	response.Reset()
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	mod := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	server.storage.Write(nil, mod)
	response := new(kvrpcpb.RawDeleteResponse)
	response.Reset()
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	kvp := make([]*kvrpcpb.KvPair, 0, req.Limit)
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := r.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	for i := 0; i < int(req.Limit); i++ {
		if !iter.Valid() {
			break
		}
		kvp = append(kvp, new(kvrpcpb.KvPair))
		kvp[i].Key = iter.Item().Key()
		kvp[i].Value, _ = iter.Item().Value()
		iter.Next()
	}
	response := new(kvrpcpb.RawScanResponse)
	response.Kvs = kvp
	return response, nil
}
