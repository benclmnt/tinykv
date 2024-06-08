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
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	b, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{
		Value:    b,
		NotFound: len(b) == 0,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	if err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}},
	}); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	if err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}},
	}); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	it := reader.IterCF(req.GetCf())
	var result []*kvrpcpb.KvPair
	for it.Seek(req.GetStartKey()); it.Valid(); it.Next() {
		if len(result) >= int(req.GetLimit()) {
			break
		}
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		result = append(result, &kvrpcpb.KvPair{
			Key:   it.Item().KeyCopy(nil),
			Value: value,
		})
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: result,
	}, nil
}
