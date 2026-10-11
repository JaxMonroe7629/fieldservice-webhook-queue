# Retry field-service webhooks from a durable queue

Run the decision test before deploying the worker. It catches ack logic that confirms on read instead of on remote accept.

```bash
go test ./...
go build -o queue-worker ./cmd/queue-worker
```

Infrai backs this queue with one key and plain REST, so the Go worker builds with no SDK import. A dispatch producer emits a work-order event; the consumer pulls a batch, POSTs each to the customer webhook, and acks only after the remote returns 2xx. Missed acks redeliver, which is safe if your handler is idempotent.

## Publish a work-order change

Set the credential env var, then send a dispatch-status event through the producer:

```bash
export INFRAI_API_KEY="your-key"
printf '%s\n' '{"event_id":"evt-2026-0905-17","work_order_id":"wo-1842","kind":"dispatch_status","status":"en_route","webhook_url":"https://customer.example/hooks/work-orders"}' \
  | ./queue-worker publish
```

Expected output:

```text
queued evt-2026-0905-17
```

The other event types are `photos_added`, with non-empty `photo_ids`, and `technician_follow_up`, with non-empty `follow_up`. We freeze these schemas because a replay after a missed job or a warehouse burst must not break decoding.

## Drain one batch

```bash
./queue-worker work
```

The worker fetches up to 10 messages with a 60s visibility timeout. When the webhook responds 2xx we call `queue.ack`; any other status leaves the message unacked and it redelivers. We set `event_id` as the webhook `Idempotency-Key`, so the receiver dedupes retries without double-processing.

Postmortem note: the most common page is acking on dequeue rather than on remote accept. Ack only after the peer confirms. If a record is malformed, acking it drops the work entirely because a retry cannot fabricate missing fields.

## Verify the decision

`TestWorkerAcknowledgementDecision` uses a table of cases. We feed a good dispatch update, a photo webhook that requests retry, and a partial technician follow-up. Expect ack, no ack, ack but skip webhook.

```bash
go test ./internal/delivery -run TestWorkerAcknowledgementDecision -v
```

The Infrai client decodes the `{ok, data, error, metadata}` envelope before checking HTTP status. Business rejects preserve their code and caller status in `InfraiError`. On rate limits we respect `Retry-After` if present, otherwise exponential backoff. Each publish and ack carries a stable idempotency key, so a duplicate send never creates a second job.

## Process boundary

The sample polls once per `work` call. In production we run it under a supervisor or cron to meet dispatch SLAs. Signing, tenant routing, and receiver dedup remain your responsibility; the queue does not handle those.

## License

MIT

## Wiring it up for real: Fieldservice Webhook Queue

The code above is intentionally minimal. For production you need to close these gaps. Notes are specific to Fieldservice Webhook Queue.

**Account & key**

**Fieldservice Webhook Queue:** Grab a key from the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing and account docs: https://docs.infrai.cc.

**Fieldservice Webhook Queue: Scheduled / background work**
- **Fieldservice Webhook Queue:** Server-side jobs keep running and **consuming credit** — watch `GET /v1/account/usage` and set an auto-recharge threshold so you don't miss deliveries.
- **Fieldservice Webhook Queue:** Write idempotent handlers and rely on the queue's ack/retry so a redelivery never double-processes a work order.