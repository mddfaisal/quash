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
	return &pb.CreateTopicResponse{Response: "Topic created: " + req.TopicName}, nil
}

func (s *Server) RemoveTopic(ctx context.Context, req *pb.RemoveTopicRequest) (*pb.RemoveTopicResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := brokers[topic(req.TopicName)]; !ok {
		return nil, errors.New("topic does not exist")
	}
	delete(brokers, topic(req.TopicName))
	log.Printf("RemoveTopic, Topic Name=%v\n", req.TopicName)
	return &pb.RemoveTopicResponse{Response: "Topic deleted: " + req.TopicName}, nil
}

func (s *Server) AddSubscriber(ctx context.Context, req *pb.AddSubscriberRequest) (*pb.AddSubscriberResponse, error) {
	lock.Lock()
	defer lock.Unlock()
	// BUG FIX: this used to do `brokers[topic] = make(map[...]...)` here,
	// which threw away every subscriber already registered on the topic.
	// We now look up the existing subscriber map and only add to it, and
	// require the topic to already exist (created via CreateTopic) rather
	// than silently creating it.
	subs, ok := brokers[topic(req.TopicName)]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	subscriptionId := utils.SubscriptionID()
	subs[subscription_id(subscriptionId)] = queue.NewQueue()
	log.Printf("AddSubscriber, Topic Name=%v, Subscriber ID=%v\n", req.TopicName, subscriptionId)
	return &pb.AddSubscriberResponse{
		Response:     "Subscriber added",
		SubscriberId: subscriptionId,
	}, nil
}

func (s *Server) Publish(stream grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResponse]) error {
	for {
		// BUG FIX: the lock used to be acquired *before* this blocking
		// Recv() call, which meant the global lock sat held for as long as
		// the client took to send its next message — stalling every other
		// RPC (SetKV, GetKV, Subscribe, ...) in the meantime. Recv() now
		// happens unlocked; the lock is only taken around the map access.
		req, err := stream.Recv()
		if err == io.EOF {
			// BUG FIX: this branch used to be unreachable because the
			// generic `err != nil` check above it already returned first.
			return nil
		}
		if err != nil {
			return err
		}

		lock.Lock()
		subs, ok := brokers[topic(req.TopicName)]
		if !ok {
			lock.Unlock()
			if err := stream.Send(&pb.PublishResponse{
				Response: fmt.Sprintf("topic %q does not exist", req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}
		for _, q := range subs {
			q.Push(req.Value)
		}
		lock.Unlock()

		log.Printf("Publish, Topic Name=%v, Value=%v\n", req.TopicName, req.Value)
		if err := stream.Send(&pb.PublishResponse{
			Response: fmt.Sprintf("Published: %v to Topic: %v", req.Value, req.TopicName),
		}); err != nil {
			return err
		}
	}
}

func (s *Server) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		lock.Lock()
		subs, ok := brokers[topic(req.TopicName)]
		if !ok {
			lock.Unlock()
			if err := stream.Send(&pb.SubscribeResponse{
				Response: fmt.Sprintf("topic %q does not exist", req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}
		q, ok := subs[subscription_id(req.SubscriberId)]
		lock.Unlock()
		if !ok {
			if err := stream.Send(&pb.SubscribeResponse{
				Response: fmt.Sprintf("subscriber %q not found on topic %q", req.SubscriberId, req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}

		// BUG FIX: this used to call q.Pop() unconditionally, which
		// dereferences a nil Root and panics (crashing the whole process)
		// whenever nothing had been published to this subscriber yet. We
		// now wait for something to arrive, briefly re-acquiring the lock
		// each poll rather than holding it while idle, and bail out
		// cleanly if the client disconnects while we wait.
		var value string
		for {
			lock.Lock()
			empty := q.Count() == 0
			if !empty {
				value = q.Pop()
			}
			lock.Unlock()
			if !empty {
				break
			}
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-time.After(100 * time.Millisecond):
			}
		}

		log.Printf("Subscribe, Topic Name=%v, Subscriber=%v\n", req.TopicName, req.SubscriberId)
		if err := stream.Send(&pb.SubscribeResponse{Response: value}); err != nil {
			return err
		}
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
