package server

import (
	"context"
	"errors"
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

// SetKV(context.Context, *SetKVRequest) (*SetKVResponse, error)
// 	GetKV(context.Context, *GetKVRequest) (*GetKVResponse, error)
// 	DeleteKV(context.Context, *DeleteKVRequest) (*DeleteKVResponse, error)
// 	CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error)
// 	RemoveTopic(context.Context, *RemoveTopicRequest) (*RemoveTopicResponse, error)
// 	AddSubscriber(*AddSubscriberRequest, grpc.ServerStreamingServer[AddSubscriberResponse]) error
// 	Subscribe(grpc.BidiStreamingServer[SubscribeRequest, SubscribeResponse]) error
// 	QueryTopicList(context.Context, *QueryTopicListRequest) (*QueueTopicListResponse, error)
// 	QueryTopicMetric(*QueryTopicMetricRequest, grpc.ServerStreamingServer[QueueTopicMetricResponse]) error

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

// Publish(grpc.BidiStreamingServer[PublishRequest, PublishResponse]) error
func (s *Server) Publish(stream grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		log.Printf("Publish, Topic Name=%v, Value=%v\n", req.TopicName, req.Value)
	}
}

func (s *Server) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		log.Printf("Subscribe, Topic Name=%v\n", req.TopicName)
	}
}

func (s *Server) QueryTopicList(ctx context.Context, q *pb.QueryTopicListRequest) (*pb.QueueTopicListResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	list := []string{}
	for k := range queues {
		list = append(list, k)
	}
	return &pb.QueueTopicListResponse{TopicList: list}, nil
}

func (s *Server) QueryTopicMetric(q *pb.QueryTopicMetricRequest, stream grpc.ServerStreamingServer[pb.QueueTopicMetricResponse]) error {
	for {
		lock.Lock()
		queueMetric := make(map[string]int64, len(queues))
		for name, qu := range queues {
			queueMetric[name] = qu.Count()
		}
		lock.Unlock()

		if err := stream.Send(&pb.QueueTopicMetricResponse{
			QueueMetric: queueMetric,
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

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	lock.Lock()
	queues[req.TopicName] = queue.NewQueue()
	log.Printf("CreateTopic, Topic Name=%v\n", req.TopicName)
	lock.Unlock()
	return &pb.CreateTopicResponse{Response: "Topic created"}, nil
}

func (s *Server) RemoveTopic(ctx context.Context, req *pb.RemoveTopicRequest) (*pb.RemoveTopicResponse, error) {
	lock.Lock()
	delete(queues, req.TopicName)
	log.Printf("RemoveTopic, Topic Name=%v\n", req.TopicName)
	lock.Unlock()
	return &pb.RemoveTopicResponse{Response: "Topic deleted"}, nil
}
