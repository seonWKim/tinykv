package server

import (
	"context"
	"log"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(context context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	ctx := kvrpcpb.Context{}
	reader, err := server.storage.Reader(&ctx)
	if err != nil {
		log.Printf("Error getting storage reader: %v", err)
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Printf("Error while GetCF, %v ", err)
		return nil, err
	}

	if value == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	mods := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}

	err := server.storage.Write(&kvrpcpb.Context{}, mods)
	if err != nil {
		log.Printf("Error on Put request, %v", err)
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	mods := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}

	err := server.storage.Write(&kvrpcpb.Context{}, mods)
	if err != nil {
		log.Printf("Error on Delete request, %v", err)
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		log.Printf("Error retrieving Reader, %v", err)
		return nil, err
	}

	pairs := []*kvrpcpb.KvPair{}

	iter := reader.IterCF(req.Cf)

	startKey := req.StartKey
	iter.Seek(startKey)

	limit := int(req.Limit)

	for i := 0; i < limit; i++ {
		if !iter.Valid() {
			break
		}

		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			log.Printf("Error retrieving key: %v, err: %v", key, err)
			break
		}

		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})

		iter.Next()
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}, nil
}