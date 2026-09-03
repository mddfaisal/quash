package admin

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mddfaisal/quash/config"
	pb "github.com/mddfaisal/quash/proto"
	"google.golang.org/grpc"
)

type PageData struct {
	Title string
}

var (
	tmpl     = template.Must(template.ParseFiles("admin/templates/index.html"))
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

func websocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("ws upgrade error:", err)
		return
	}
	defer conn.Close()

	grpcConn, err := grpc.Dial("0.0.0.0:6300", grpc.WithInsecure())
	if err != nil {
		log.Println("grpc dial error:", err)
		return
	}
	defer grpcConn.Close()

	client := pb.NewQuashServiceClient(grpcConn)
	stream, err := client.QueryQueueMetric(r.Context(), &pb.QueryQueueMetricRequest{})
	if err != nil {
		log.Println("stream error:", err)
		return
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Println("stream recv error:", err)
			return
		}
		data, _ := json.Marshal(resp.QueueMatric)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Println("ws write error:", err)
			return
		}
		time.Sleep(500 * time.Millisecond) // throttle push rate
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		Title: "Admin",
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Println("template execute error:", err)
	}
}

func AdminServer() {
	http.HandleFunc("/ws/admin", websocketHandler)
	fs := http.FileServer(http.Dir("./admin/templates/"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	http.HandleFunc("/", indexHandler)
	log.Println("admin server listening on", config.QuashTelemetry)
	http.ListenAndServe(config.QuashTelemetry, nil)
}
