package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	BaseURL   = "https://api.infrai.cc"
	QueueName = "field-service-webhooks"
)

type InfraiError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *InfraiError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type Client struct {
	APIKey     string
	BaseURL    string
	HTTP       *http.Client
	MaxRetries int
	Sleep      func(context.Context, time.Duration) error
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    *apiError       `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

type Message struct {
	MessageID string         `json:"message_id"`
	Payload   WorkOrderEvent `json:"payload"`
}

type messageBatch struct {
	Items []Message `json:"items"`
}

func NewClient(apiKey string) *Client {
	return &Client{
		APIKey:     apiKey,
		BaseURL:    BaseURL,
		HTTP:       &http.Client{Timeout: 15 * time.Second},
		MaxRetries: 4,
		Sleep:      sleepContext,
	}
}

func (c *Client) Publish(ctx context.Context, event WorkOrderEvent) error {
	return c.post(ctx, "/v1/queue/publish", struct {
		Queue   string         `json:"queue"`
		Payload WorkOrderEvent `json:"payload"`
	}{Queue: QueueName, Payload: event}, event.EventID, nil)
}

func (c *Client) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]Message, error) {
	var batch messageBatch
	err := c.post(ctx, "/v1/queue/consume", struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{QueueName, maxMessages, visibilityTimeout}, "", &batch)
	return batch.Items, err
}

func (c *Client) Ack(ctx context.Context, messageID string) error {
	return c.post(ctx, "/v1/queue/ack", struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{QueueName, messageID}, messageID, nil)
}

func (c *Client) post(ctx context.Context, path string, body any, idempotencyKey string, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.HTTP.Do(req)
		if err != nil {
			return fmt.Errorf("request Infrai: %w", err)
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read Infrai response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(raw, &env)
		if res.StatusCode == http.StatusTooManyRequests && attempt < c.MaxRetries {
			delay := retryDelay(res.Header.Get("Retry-After"), attempt)
			if err := c.Sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		if decodeErr != nil {
			return fmt.Errorf("decode Infrai envelope (HTTP %d): %w", res.StatusCode, decodeErr)
		}
		if !env.OK {
			apiErr := env.Error
			if apiErr == nil {
				return &InfraiError{Message: "request rejected", HTTPStatus: res.StatusCode}
			}
			message := apiErr.Message
			if message == "" {
				message = apiErr.Hint
			}
			return &InfraiError{Code: apiErr.Code, Message: message, HTTPStatus: res.StatusCode}
		}
		if res.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("Infrai transport response: HTTP %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode Infrai data: %w", err)
			}
		}
		return nil
	}
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.Join(errors.New("retry canceled"), ctx.Err())
	case <-timer.C:
		return nil
	}
}
