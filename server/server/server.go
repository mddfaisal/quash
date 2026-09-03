package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/queue"
	grpc "google.golang.org/grpc"
)

type kvValue struct {
	value    string
	timeOut  time.Duration
	initTime time.Time
}

var (
	lock   = &sync.Mutex{}
	kvMap  map[string]kvValue
	queues = map[string]*queue.Queue{}
)

type Server struct {
	pb.QuashServiceServer
}

func (s *Server) Init() {
	go GarbageCollection()
}

func (s *Server) SetKV(ctx context.Context, kv *pb.SetKVRequest) (*pb.SetKVResponse, error) {
	log.Printf("SetKV, key=%v, value=%v,\n", kv.Key, kv.Value)
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
	defer lock.Unlock()
	val, ok := kvMap[kv.Key]
	if ok {
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
		lock.Lock()
		delete(kvMap, req.Key)
		lock.Unlock()
	}
	return &pb.DeleteKVResponse{Value: "OK"}, nil
}

func (s *Server) PushQueue(ctx context.Context, q *pb.PushIntoQueueRequest) (*pb.PushIntoQueueResponse, error) {
	log.Printf("PushQueue, Queue Name=%v, Value=%v\n", q.QueueName, q.Value)
	lock.Lock()
	_, ok := queues[q.QueueName]
	if !ok {
		queues[q.QueueName] = queue.NewQueue()
	}
	queues[q.QueueName].Push(q.Value)
	lock.Unlock()
	return &pb.PushIntoQueueResponse{Response: "Pushed"}, nil
}

func (s *Server) PopQueue(q *pb.PopFromQueueRequest, stream grpc.ServerStreamingServer[pb.PopFromQueueResponse]) error {
	log.Printf("PopQueue, Queue Name=%v\n", q.QueueName)
	for {
		lock.Lock()
		queue, ok := queues[q.QueueName]
		if !ok {
			return errors.New("queue doesn't exist")
		}
		if queue.Count() == 0 {
			continue
		}
		first := queues[q.QueueName].Pop()
		lock.Unlock()
		err := stream.Send(&pb.PopFromQueueResponse{
			Response: fmt.Sprintf("%v", first),
		})
		if err != nil {
			return err
		}
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
		var queueMetric = map[string]int64{}
		for queue, totElements := range queues {
			queueMetric[queue] = totElements.Count()
		}
		stream.Send(&pb.QueueMetricResponse{
			QueueMatric: queueMetric,
		})
	}
}

func (s *Server) memoryDump() {

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

func (s *Server) Shutdown() {
	s.memoryDump()
	log.Printf("gRPC server stopped gracefully\n")
}

func (s *Server) CreateQueue(ctx context.Context, req *pb.CreateQueueRequest) (*pb.CreateQueueResponse, error) {
	lock.Lock()
	queues[req.QueueName] = queue.NewQueue()
	log.Printf("CreateQueue, Queue Name=%v\n", req.QueueName)
	lock.Unlock()
	return &pb.CreateQueueResponse{Response: "Queue created"}, nil
}

func (s *Server) DeleteQueue(ctx context.Context, req *pb.DeleteQueueRequest) (*pb.DeleteQueueResponse, error) {
	lock.Lock()
	delete(queues, req.QueueName)
	log.Printf("DeleteQueue, Queue Name=%v\n", req.QueueName)
	lock.Unlock()
	return &pb.DeleteQueueResponse{Response: "Queue deleted"}, nil
}
