package server

import (
	"net"

	"github.com/mddfaisal/quash/admin"
	"github.com/mddfaisal/quash/config"
	pb "github.com/mddfaisal/quash/proto"
	"google.golang.org/grpc"

	"github.com/mddfaisal/quash/server/server"
)

var (
	srv      = grpc.NewServer()
	quashSrv = &server.Server{}
)

func Serve() {
	go admin.AdminServer()
	lis, err := net.Listen("tcp", config.QuashDb)
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
