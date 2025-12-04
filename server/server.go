package server

import (
	"net"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/server"
	"google.golang.org/grpc"
)

const addr = "0.0.0.0:6300"

func Serve() {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	srv := grpc.NewServer()
	pb.RegisterQuashServiceServer(srv, &server.Server{})
	if err := srv.Serve(lis); err != nil {
		panic(err)
	}
}
