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
	Items []string
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	tmpl = template.Must(template.New("index").Parse(`
<!DOCTYPE html>
<html>
<title>{{.Title}}</title>
  <style>
    body {
      font-family: Arial, sans-serif;
    }

    header {
      background-color: #f0f0f0;
      padding: 10px;
      text-align: left;
    }

    ul {
      list-style-type: none;
      padding: 0;
    }

    li {
      background-color: #e0e0e0;
      margin: 5px 0;
      padding: 10px;
    }
  </style>
  <script>
    const ws = new WebSocket("ws://localhost:6301/ws/admin");
    ws.onmessage = (e) => {
      const metrics = JSON.parse(e.data);
      console.log("live queue metrics:", metrics);
    };
  </script>
<body>
  <h1>{{.Title}}</h1>
  <ul>
    {{range .Items}}<li>{{.}}</li>{{end}}
  </ul>
</body>
</html>
`))
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
	stream, err := client.QueryTopicMetric(r.Context(), &pb.QueryTopicMetricRequest{})
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
		data, _ := json.Marshal(resp.QueueMetric)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Println("ws write error:", err)
			return
		}
		time.Sleep(500 * time.Millisecond) // throttle push rate
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{Title: "Hello", Items: []string{"a", "b", "c"}}
	tmpl.Execute(w, data)
}

func AdminServer() {
	http.HandleFunc("/ws/admin", websocketHandler)
	http.HandleFunc("/", indexHandler)
	log.Println("admin server listening on", config.QuashTelemetry)
	http.ListenAndServe(config.QuashTelemetry, nil)
}
