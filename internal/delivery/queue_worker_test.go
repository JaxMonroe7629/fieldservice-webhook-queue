package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type queueStub struct {
	messages []Message
	acked    []string
}

func (q *queueStub) Consume(context.Context, int, int) ([]Message, error) {
	return q.messages, nil
}

func (q *queueStub) Ack(_ context.Context, id string) error {
	q.acked = append(q.acked, id)
	return nil
}

func TestWorkerAcknowledgementDecision(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		event      WorkOrderEvent
		wantAck    bool
		wantCalls  int
	}{
		{"delivered dispatch update", http.StatusNoContent, WorkOrderEvent{EventID: "evt-1", WorkOrderID: "wo-41", Kind: "dispatch_status", Status: "en_route"}, true, 1},
		{"receiver asks for retry", http.StatusConflict, WorkOrderEvent{EventID: "evt-2", WorkOrderID: "wo-42", Kind: "photos_added", PhotoIDs: []string{"photo-8"}}, false, 1},
		{"incomplete follow-up is consumed", http.StatusNoContent, WorkOrderEvent{EventID: "evt-3", WorkOrderID: "wo-43", Kind: "technician_follow_up"}, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if got := r.Header.Get("Idempotency-Key"); got != tt.event.EventID {
					t.Fatalf("idempotency key = %q", got)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			tt.event.WebhookURL = server.URL
			queue := &queueStub{messages: []Message{{MessageID: "msg-1", Payload: tt.event}}}
			worker := Worker{Queue: queue, HTTP: server.Client()}
			_, err := worker.RunOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := []string(nil)
			if tt.wantAck {
				want = []string{"msg-1"}
			}
			if !reflect.DeepEqual(queue.acked, want) {
				t.Fatalf("acked = %v, want %v", queue.acked, want)
			}
			if calls != tt.wantCalls {
				t.Fatalf("webhook calls = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}
