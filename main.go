package main

import (
	"github.com/mddfaisal/quash/server"
)

func main() {
	// go func() {
	// 	time.Sleep(2 * time.Second) // wait for server to start
	// 	for i := 0; i < 10000000000; i++ {
	// 		resp, err := client.SetKV(context.Background(), &quash_proto.SetKVRequest{
	// 			Key:   fmt.Sprintf("%v", i),
	// 			Value: fmt.Sprintf("%v", i),
	// 		})
	// 		fmt.Println(resp, err)
	// 	}
	// }()
	server.Serve()
}
