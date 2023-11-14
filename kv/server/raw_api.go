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
	// 获取reader
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawGetResponse{}

	// 进行点查
	values, err := reader.GetCF(req.GetCf(), req.GetKey())

	// 调用失败
	if err != nil {
		response.Error = err.Error()
		return nil, err
	}

	if values == nil {
		response.NotFound = true
	}

	response.Value = values
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawScanResponse{}

	// 进行范围查询
	iterator := reader.IterCF(req.GetCf())

	limit := req.GetLimit()
	response.Kvs = make([]*kvrpcpb.KvPair, 0)
	var cnt uint32 = 0
	for iterator.Seek([]byte{1}); iterator.Valid() && cnt < limit; iterator.Next() {
		item := iterator.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		cnt++
	}

	return response, nil
}
