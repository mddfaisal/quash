package quashserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/queue"
	"github.com/mddfaisal/quash/utils"
	grpc "google.golang.org/grpc"
)

type kvValue struct {
	value    string
	timeOut  time.Duration
	initTime time.Time
}

type topic string
type subscription_id string

var (
	lock    = &sync.Mutex{}
	kvMap   = map[string]kvValue{}
	brokers = map[topic]map[subscription_id]*queue.Queue{}
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

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := brokers[topic(req.TopicName)]; ok {
		return nil, errors.New("topic already exists")
	}
	brokers[topic(req.TopicName)] = map[subscription_id]*queue.Queue{}
	log.Printf("CreateTopic, Topic Name=%v\n", req.TopicName)
	return &pb.CreateTopicResponse{Response: "Topic created: " + fmt.Sprintf("%s", req.TopicName)}, nil
}

func (s *Server) RemoveTopic(ctx context.Context, req *pb.RemoveTopicRequest) (*pb.RemoveTopicResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := brokers[topic(req.TopicName)]; !ok {
		return nil, errors.New("topic does not exist")
	}
	delete(brokers, topic(req.TopicName))
	log.Printf("RemoveTopic, Topic Name=%v\n", req.TopicName)
	return &pb.RemoveTopicResponse{Response: "Topic deleted: " + fmt.Sprintf("%s", req.TopicName)}, nil
}

func (s *Server) AddSubscriber(ctx context.Context, req *pb.AddSubscriberRequest) (*pb.AddSubscriberResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	subscriptionId := utils.SubscriptionID()
	brokers[topic(req.TopicName)] = make(map[subscription_id]*queue.Queue)
	brokers[topic(req.TopicName)][subscription_id(subscriptionId)] = queue.NewQueue()
	log.Printf("AddSubscriber, Topic Name=%v, Subscriber ID=%v\n", req.TopicName, subscription_id(subscriptionId))
	return &pb.AddSubscriberResponse{
		Response:     "Subscriber added",
		SubscriberId: subscriptionId,
	}, nil
}

func (s *Server) Publish(stream grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResponse]) error {
	for {
		lock.Lock()
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if err == io.EOF {
			return nil
		}
		topics := brokers[topic(req.TopicName)]
		for _, q := range topics {
			q.Push(req.Value)
		}
		err = stream.Send(&pb.PublishResponse{
			Response: fmt.Sprintf("Published: %v to Topic: %v", req.Value, req.TopicName),
		})
		if err != nil {
			panic(err)
		}
		log.Printf("Publish, Topic Name=%v, Value=%v\n", req.TopicName, req.Value)
		lock.Unlock()
	}
}

func (s *Server) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if val, ok1 := brokers[topic(req.TopicName)]; ok1 {
			if q, ok2 := val[subscription_id(req.SubscriberId)]; ok2 {
				stream.Send(&pb.SubscribeResponse{
					Response: q.Pop(),
				})
				continue
			}
		}
		if err != nil {
			panic(err)
		}
		log.Printf("Subscribe, Topic Name=%v\n", req.TopicName)
	}
}

func (s *Server) QueryTopicList(ctx context.Context, q *pb.QueryTopicListRequest) (*pb.QueueTopicListResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	list := []string{}
	for k := range brokers {
		list = append(list, string(k))
	}
	return &pb.QueueTopicListResponse{TopicList: list}, nil
}

func (s *Server) QueryTopicMetric(q *pb.QueryTopicMetricRequest, stream grpc.ServerStreamingServer[pb.QueueTopicMetricResponse]) error {
	for {
		lock.Lock()
		queueMetric := make(map[string]*pb.QueueMetric, len(brokers))
		for name, qu := range brokers {
			queueMetric[string(name)] = &pb.QueueMetric{
				QueueMetric: map[string]int64{
					"subscriber_count": int64(len(qu)),
				},
			}
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
