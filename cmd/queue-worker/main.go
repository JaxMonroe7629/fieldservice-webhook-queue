package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/example/fieldservice-webhook-worker/internal/delivery"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: queue-worker publish|work")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	client := delivery.NewClient(key)
	ctx := context.Background()

	switch os.Args[1] {
	case "publish":
		var event delivery.WorkOrderEvent
		if err := json.NewDecoder(os.Stdin).Decode(&event); err != nil {
			log.Fatal(err)
		}
		if !delivery.ShouldDeliver(event) {
			log.Fatal("event is incomplete or has an unsupported kind")
		}
		if err := client.Publish(ctx, event); err != nil {
			log.Fatal(err)
		}
		fmt.Println("queued", event.EventID)
	case "work":
		worker := delivery.Worker{Queue: client, HTTP: &http.Client{Timeout: 15 * time.Second}}
		acked, err := worker.RunOnce(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("acknowledged %d messages\n", acked)
	default:
		log.Fatal("usage: queue-worker publish|work")
	}
}
