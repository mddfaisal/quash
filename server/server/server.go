package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/queue"
	grpc "google.golang.org/grpc"
)

var (
	bytes_in_gb       uint64 = 1073741824
	kvDump, queueDump *os.File
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

func (s *Server) Init() {
	go s.ManageMemory()
	go GarbageCollection()
	var err error
	kvDump, err = os.Create(".kv_dump")
	if err != nil {
		panic(err)
	}
	queueDump, err = os.Create(".queue_dump")
	if err != nil {
		panic(err)
	}
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
	now := time.Now()
	lock.Lock()
	val, ok := kvMap[kv.Key]
	if ok {
		if now.After(val.initTime.Add(val.timeOut)) {
			delete(kvMap, kv.Key)
			return nil, errors.New("key does't exists")
		}
		return &pb.GetKVResponse{Value: val.value}, nil
	} else {
		var fsKvMap map[string]kvValue
		var fsKvMapBytes []byte
		_, err := kvDump.Read(fsKvMapBytes)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(fsKvMapBytes, &fsKvMap)
		if err != nil {
			panic(err)
		}
		val, ok := fsKvMap[kv.Key]
		if ok {
			if now.After(val.initTime.Add(val.timeOut)) {
				delete(fsKvMap, kv.Key)
				return nil, errors.New("key does't exists")
			}
			return &pb.GetKVResponse{Value: val.value}, nil
		}
	}
	return nil, errors.New("key does't exists")
}

func (s *Server) DeleteKV(ctx context.Context, req *pb.DeleteKVRequest) (*pb.DeleteKVResponse, error) {
	_, ok := kvMap[req.Key]
	if ok {
		lock.Lock()
		delete(kvMap, req.Key)
		lock.Unlock()
	} else {
		var fsKvMap map[string]kvValue
		var fsKvMapBytes []byte
		_, err := kvDump.Read(fsKvMapBytes)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(fsKvMapBytes, &fsKvMap)
		if err != nil {
			panic(err)
		}
		_, ok := fsKvMap[req.Key]
		if ok {
			delete(fsKvMap, req.Key)
			return &pb.DeleteKVResponse{Value: "OK"}, nil
		}
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
		log.Printf("\nHeap Allocation: %v", heapAlloc)
		lock.Unlock()
		if heapAlloc >= bytes_in_gb {
			log.Printf("\nHeap memory usage is high: %v bytes\n", heapAlloc)
			data, err := json.Marshal(kvMap)
			if err != nil {
				panic(err)
			}
			_, err = kvDump.Write(data)
			if err != nil {
				panic(err)
			}
			kvDump.Sync()
			log.Printf("Dumped kvMap to .kv_dump file\n")
			// Clear the in-memory map
			lock.Lock()
			kvMap = make(map[string]kvValue)
			lock.Unlock()
			log.Printf("Cleared in-memory kvMap\n")
			// Dump queues
			queueData, err := json.Marshal(queues)
			if err != nil {
				panic(err)
			}
			_, err = queueDump.Write(queueData)
			if err != nil {
				panic(err)
			}
			queueDump.Sync()
			log.Printf("Dumped queues to .queue_dump file\n")
			// Clear the in-memory queues
			lock.Lock()
			queues = make(map[string]*queue.Queue)
			lock.Unlock()
			log.Printf("Cleared in-memory queues\n")
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
		lock.Lock()
		var fsKvMap map[string]kvValue
		var fsKvMapBytes []byte
		_, err := kvDump.Read(fsKvMapBytes)
		if len(fsKvMapBytes) == 0 {
			lock.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(fsKvMapBytes, &fsKvMap)
		if err != nil {
			panic(err)
		}
		for k, v := range fsKvMap {
			now := time.Now()
			if now.After(v.initTime.Add(v.timeOut)) {
				delete(kvMap, k)
			}
		}
		lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
