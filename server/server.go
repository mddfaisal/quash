package server

import (
	"net"

	"github.com/mddfaisal/quash/admin"
	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/quashserver"
	"github.com/mddfaisal/quash/utils"
	"google.golang.org/grpc"
)

var (
	srv      = grpc.NewServer()
	quashSrv = &quashserver.Server{}
)

func Serve() {
	go admin.AdminServer()
	lis, err := net.Listen("tcp", utils.QuashDb)
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
