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
	"github.com/mddfaisal/quash/structs"
	"github.com/mddfaisal/quash/utils"
	grpc "google.golang.org/grpc"
)

var (
	// Split from a single global lock: kvLock guards kvMap, brokersLock
	// guards the topic/subscriber map. No RPC ever needs to hold both at
	// once, so splitting them removes contention between KV operations
	// and pub/sub operations without introducing any lock-ordering /
	// deadlock risk.
	kvLock      = &sync.Mutex{}
	brokersLock = &sync.Mutex{}

	kvMap = map[string]structs.KVValue{}

	// Each subscriber's "mailbox" is now a native Go channel instead of
	// the linked-list queue.Queue. This gets us blocking, event-driven
	// delivery for free via `select` — no manual polling loop, no risk of
	// popping from an empty structure.
	brokers = map[structs.Topic]map[structs.SubscriptionID]chan string{}
)

// BUG FIX: GetKVMapLength/GetBrokers used to return the live kvMap /
// brokers map by reference *after* releasing the lock that was
// protecting it. Any caller ranging over that returned map (e.g. to
// json.Marshal it) was doing so completely unprotected while Publish,
// AddSubscriber, CreateTopic, etc. concurrently wrote to the real map
// under brokersLock — a textbook "concurrent map iteration and map
// write" fatal error. Snapshot() replaces both: it builds a brand new,
// independent map while holding each lock, so what it returns is a
// point-in-time copy that's safe to read (or marshal) with no lock at
// all afterward.
func Snapshot() structs.SrvData {
	kvLock.Lock()
	kvLen := int64(len(kvMap))
	kvLock.Unlock()

	brokersLock.Lock()
	topics := make(map[string]map[string]int64, len(brokers))
	for name, subs := range brokers {
		topics[string(name)] = make(map[string]int64)
		for subscriber, ch := range subs {
			topics[string(name)][string(subscriber)] = int64(len(ch))
		}
	}
	brokersLock.Unlock()

	return structs.SrvData{
		KVMapLength: kvLen,
		Topics:      topics,
	}
}

type Server struct {
	pb.QuashServiceServer
}

func (s *Server) Init() {
	go GarbageCollection()
}

func (s *Server) SetKV(ctx context.Context, kv *pb.SetKVRequest) (*pb.SetKVResponse, error) {
	log.Printf("SetKV, key=%v, value=%v,\n", kv.Key, kv.Value)
	kvLock.Lock()
	kvMap[kv.Key] = structs.KVValue{
		Value:    kv.Value,
		InitTime: time.Now(),
		TimeOut:  time.Duration(kv.TimeOutDuration) * time.Second,
	}
	kvLock.Unlock()
	return &pb.SetKVResponse{Response: "Added"}, nil
}

func (s *Server) GetKV(ctx context.Context, kv *pb.GetKVRequest) (*pb.GetKVResponse, error) {
	log.Printf("GetKV, key=%v\n", kv.Key)
	now := time.Now()
	kvLock.Lock()
	defer kvLock.Unlock()
	val, ok := kvMap[kv.Key]
	if !ok {
		return nil, errors.New("key doesn't exist")
	}
	if now.After(val.InitTime.Add(val.TimeOut)) {
		delete(kvMap, kv.Key)
		return nil, errors.New("key doesn't exist")
	}
	return &pb.GetKVResponse{Value: val.Value}, nil
}

func (s *Server) DeleteKV(ctx context.Context, req *pb.DeleteKVRequest) (*pb.DeleteKVResponse, error) {
	kvLock.Lock()
	defer kvLock.Unlock()
	delete(kvMap, req.Key)
	return &pb.DeleteKVResponse{Value: "OK"}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	if _, ok := brokers[structs.Topic(req.TopicName)]; ok {
		return nil, errors.New("topic already exists")
	}
	brokers[structs.Topic(req.TopicName)] = map[structs.SubscriptionID]chan string{}
	log.Printf("CreateTopic, Topic Name=%v\n", req.TopicName)
	return &pb.CreateTopicResponse{Response: "Topic created: " + req.TopicName}, nil
}

func (s *Server) RemoveTopic(ctx context.Context, req *pb.RemoveTopicRequest) (*pb.RemoveTopicResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	subs, ok := brokers[structs.Topic(req.TopicName)]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	// Closing each channel wakes up every Subscribe call currently
	// blocked on this topic so they can exit instead of hanging forever.
	for _, ch := range subs {
		close(ch)
	}
	delete(brokers, structs.Topic(req.TopicName))
	log.Printf("RemoveTopic, Topic Name=%v\n", req.TopicName)
	return &pb.RemoveTopicResponse{Response: "Topic deleted: " + req.TopicName}, nil
}

func (s *Server) AddSubscriber(ctx context.Context, req *pb.AddSubscriberRequest) (*pb.AddSubscriberResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	subs, ok := brokers[structs.Topic(req.TopicName)]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	id := utils.SubscriptionID()
	subs[structs.SubscriptionID(id)] = make(chan string, utils.SubscriberBufferSize)
	log.Printf("AddSubscriber, Topic Name=%v, Subscriber ID=%v\n", req.TopicName, id)
	return &pb.AddSubscriberResponse{
		Response:     "Subscriber added",
		SubscriberId: id,
	}, nil
}

func (s *Server) Publish(stream grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		brokersLock.Lock()
		subs, ok := brokers[structs.Topic(req.TopicName)]
		if !ok {
			brokersLock.Unlock()
			if err := stream.Send(&pb.PublishResponse{
				Response: fmt.Sprintf("topic %q does not exist", req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}
		for id, ch := range subs {
			select {
			case ch <- req.Value:
			default:
				// Mailbox full — drop rather than block the publisher on
				// one slow subscriber.
				log.Printf("Publish, dropping message for slow subscriber=%v on topic=%v\n", id, req.TopicName)
			}
		}
		brokersLock.Unlock()

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

		brokersLock.Lock()
		subs, ok := brokers[structs.Topic(req.TopicName)]
		if !ok {
			brokersLock.Unlock()
			if err := stream.Send(&pb.SubscribeResponse{
				Response: fmt.Sprintf("topic %q does not exist", req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}
		ch, ok := subs[structs.SubscriptionID(req.SubscriberId)]
		brokersLock.Unlock()
		if !ok {
			if err := stream.Send(&pb.SubscribeResponse{
				Response: fmt.Sprintf("subscriber %q not found on topic %q", req.SubscriberId, req.TopicName),
			}); err != nil {
				return err
			}
			continue
		}

		// Blocks here with no CPU usage until Publish sends a value, the
		// topic is removed (channel closed), or the client disconnects —
		// no lock held, no polling.
		select {
		case value, open := <-ch:
			if !open {
				if err := stream.Send(&pb.SubscribeResponse{
					Response: fmt.Sprintf("topic %q was removed", req.TopicName),
				}); err != nil {
					return err
				}
				continue
			}
			log.Printf("Subscribe, Topic Name=%v, Subscriber=%v\n", req.TopicName, req.SubscriberId)
			if err := stream.Send(&pb.SubscribeResponse{Response: value}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *Server) memoryDump() {

}

func GarbageCollection() {
	for {
		kvLock.Lock()
		now := time.Now()
		for k, v := range kvMap {
			if now.After(v.InitTime.Add(v.TimeOut)) {
				delete(kvMap, k)
			}
		}
		kvLock.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (s *Server) Shutdown() {
	s.memoryDump()
	log.Printf("gRPC server stopped gracefully\n")
}
