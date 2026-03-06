package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"time"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/queue"
	grpc "google.golang.org/grpc"
)

var (
	bytes_in_gb uint64 = 1073741824
	kvDump := 
	queueDump := 
)

type kvValue struct {
	value    string
	timeOut  time.Duration
	initTime time.Time
}

var (
	kvMap  map[string]kvValue
	lock   = &sync.Mutex{}
	queues = map[string]*queue.Queue{}
)

type Server struct {
	pb.QuashServiceServer
}

func (s *Server) SetKV(ctx context.Context, kv *pb.SetKVRequest) (*pb.SetKVResponse, error) {
	log.Printf("SetKV, key=%v, value=%v\n", kv.Key, kv.Value)
	lock.Lock()
	if len((kvMap)) == 0 {
		kvMap = make(map[string]kvValue)
	}
	kvMap[kv.Key] = kvValue{
		value:    kv.Value,
		initTime: time.Now(),
		timeOut:  time.Duration(kv.InitTime),
	}
	lock.Unlock()
	return &pb.SetKVResponse{Response: "Added"}, nil
}

func (s *Server) GetKV(ctv context.Context, kv *pb.GetKVRequest) (*pb.GetKVResponse, error) {
	log.Printf("GetKV, key=%v\n", kv.Key)
	lock.Lock()
	val, ok := kvMap[kv.Key]
	if ok {
		now := time.Now()
		if now.After(val.initTime.Add(val.timeOut)) {
			delete(kvMap, kv.Key)
			return nil, errors.New("key does't exists")
		}
		return &pb.GetKVResponse{Value: val.value}, nil
	}
	return nil, errors.New("key does't exists")
}

func (s *Server) DeleteKV(ctx context.Context, req *pb.DeleteKVRequest) (*pb.DeleteKVResponse, error) {
	_, ok := kvMap[req.Key]
	if ok {
		delete(kvMap, req.Key)
	}
	return &pb.DeleteKVResponse{Value: "OK"}, nil
}

func (s *Server) PushQueue(ctx context.Context, q *pb.PushIntoQueueRequest) (*pb.PushIntoQueueResponse, error) {
	log.Printf("PushQueue, Queue Name=%v, Value=%v\n", q.QueueName, q.Value)
	lock.Lock()
	queues[q.QueueName].Push(q.Value)
	lock.Unlock()
	return &pb.PushIntoQueueResponse{Response: "Pushed"}, nil
}

func (s *Server) PopQueue(q *pb.PopFromQueueRequest, stream grpc.ServerStreamingServer[pb.PopFromQueueResponse]) error {
	for {
		var m interface{}
		err := stream.RecvMsg(&m)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(m)
	}
}

func (s *Server) QueryQueueList(ctx context.Context, q *pb.QueryQueueListRequest) (*pb.QueueListResponse, error) {
	list := []string{}
	for k := range queues {
		list = append(list, k)
	}
	return &pb.QueueListResponse{QueueList: list}, nil
}

func (s *Server) QueryQueueMetric(q *pb.QueryQueueMetricRequest, stream grpc.ServerStreamingServer[pb.QueueMetricResponse]) error {
	for {
		var m interface{}
		err := stream.RecvMsg(&m)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(m)
	}
}

func (s *Server) ManageMemory() {
	for {
		lock.Lock()
		heapAlloc := heapMemoryAlloc()
		lock.Unlock()
		fmt.Println(heapAlloc)
		if heapAlloc >= bytes_in_gb {
			// dump data
		}
		time.Sleep(1 * time.Second)
	}
}

func heapMemoryAlloc() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func GarbageCollection() {
	for {
		lock.Lock()
		for k, v := range kvMap {
			now := time.Now()
			if now.After(v.initTime.Add(v.timeOut)) {
				delete(kvMap, k)
			}
		}
		lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
