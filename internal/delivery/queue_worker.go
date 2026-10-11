package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type WorkOrderEvent struct {
	EventID     string   `json:"event_id"`
	WorkOrderID string   `json:"work_order_id"`
	Kind        string   `json:"kind"`
	PhotoIDs    []string `json:"photo_ids,omitempty"`
	Status      string   `json:"status,omitempty"`
	FollowUp    string   `json:"follow_up,omitempty"`
	WebhookURL  string   `json:"webhook_url"`
}

type Queue interface {
	Consume(context.Context, int, int) ([]Message, error)
	Ack(context.Context, string) error
}

type Worker struct {
	Queue Queue
	HTTP  *http.Client
}

func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	messages, err := w.Queue.Consume(ctx, 10, 60)
	if err != nil {
		return 0, err
	}
	acked := 0
	for _, message := range messages {
		if !ShouldDeliver(message.Payload) {
			if err := w.Queue.Ack(ctx, message.MessageID); err != nil {
				return acked, err
			}
			acked++
			continue
		}
		if err := w.deliver(ctx, message.Payload); err != nil {
			continue
		}
		if err := w.Queue.Ack(ctx, message.MessageID); err != nil {
			return acked, err
		}
		acked++
	}
	return acked, nil
}

func ShouldDeliver(event WorkOrderEvent) bool {
	if event.EventID == "" || event.WorkOrderID == "" || event.WebhookURL == "" {
		return false
	}
	switch event.Kind {
	case "photos_added":
		return len(event.PhotoIDs) > 0
	case "dispatch_status":
		return event.Status != ""
	case "technician_follow_up":
		return event.FollowUp != ""
	default:
		return false
	}
}

func (w *Worker) deliver(ctx context.Context, event WorkOrderEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, event.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", event.EventID)
	res, err := w.HTTP.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", res.StatusCode)
	}
	return nil
}
