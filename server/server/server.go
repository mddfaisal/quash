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
	kvMap  = map[string]kvValue{}
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
	kvMap[kv.Key] = kvValue{
		value:    kv.Value,
		initTime: time.Now(),
		timeOut:  time.Duration(kv.TimeOutDuration) * time.Second,
	}
	lock.Unlock()
	return &pb.SetKVResponse{Response: "Added"}, nil
}

func (s *Server) GetKV(ctx context.Context, kv *pb.GetKVRequest) (*pb.GetKVResponse, error) {
	log.Printf("GetKV, key=%v\n", kv.Key)
	now := time.Now()
	lock.Lock()
	defer lock.Unlock()
	val, ok := kvMap[kv.Key]
	if !ok {
		return nil, errors.New("key doesn't exist")
	}
	if now.After(val.initTime.Add(val.timeOut)) {
		delete(kvMap, kv.Key)
		return nil, errors.New("key doesn't exist")
	}
	return &pb.GetKVResponse{Value: val.value}, nil
}

func (s *Server) DeleteKV(ctx context.Context, req *pb.DeleteKVRequest) (*pb.DeleteKVResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	delete(kvMap, req.Key)
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
		q2, ok := queues[q.QueueName]
		if !ok {
			lock.Unlock()
			return errors.New("queue doesn't exist")
		}
		if q2.Count() == 0 {
			lock.Unlock()
			select {
			case <-stream.Context().Done():
				// client disconnected while we were waiting; stop the goroutine
				return stream.Context().Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		first := q2.Pop()
		lock.Unlock()

		if err := stream.Send(&pb.PopFromQueueResponse{
			Response: fmt.Sprintf("%v", first),
		}); err != nil {
			return err
		}
	}
}

func (s *Server) QueryQueueList(ctx context.Context, q *pb.QueryQueueListRequest) (*pb.QueueListResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	list := []string{}
	for k := range queues {
		list = append(list, k)
	}
	return &pb.QueueListResponse{QueueList: list}, nil
}

func (s *Server) QueryQueueMetric(q *pb.QueryQueueMetricRequest, stream grpc.ServerStreamingServer[pb.QueueMetricResponse]) error {
	for {
		lock.Lock()
		queueMetric := make(map[string]int64, len(queues))
		for name, qu := range queues {
			queueMetric[name] = qu.Count()
		}
		lock.Unlock()

		if err := stream.Send(&pb.QueueMetricResponse{
			QueueMatric: queueMetric,
		}); err != nil {
			return err
		}

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Server) memoryDump() {

}

func GarbageCollection() {
	for {
		lock.Lock()
		now := time.Now()
		for k, v := range kvMap {
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
