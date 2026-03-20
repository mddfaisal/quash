package server

import (
	"net"

	pb "github.com/mddfaisal/quash/proto"
	"google.golang.org/grpc"

	"github.com/mddfaisal/quash/server/server"
)

const addr = "0.0.0.0:6300"

var (
	srv      = grpc.NewServer()
	quashSrv = &server.Server{}
)

func Serve() {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	quashSrv.Init()
	pb.RegisterQuashServiceServer(srv, quashSrv)
	if err := srv.Serve(lis); err != nil {
		panic(err)
	}
}

func Shutdown() {
	quashSrv.Shutdown()
	srv.GracefulStop()
}
