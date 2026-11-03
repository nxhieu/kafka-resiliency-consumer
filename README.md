# kafka-resiliency-consumer

A small Kafka consumer that saves product prices. It exists to show one thing: what a consumer
should do when processing a message fails.

It's rebuilt from scratch, with a made-up domain, from resiliency work I did on a production
Kafka-to-document-database pipeline. No employer code.

## The problem

The production consumer treated every failure the same way. A missing `await` let errors escape
as unhandled rejections. A database timeout, a malformed payload and a read-then-write race all
ended the same way: the process crashed, Kubernetes restarted it with exponential backoff, and
lag built up while it was down.

But those failures aren't the same. Some go away if you try again, some never will, and some mean
the code itself is wrong.

## The rule

| What failed | Example | What the consumer does |
|---|---|---|
| **Transient** | Database timeout, dropped connection, write conflict | Retry up to 5 times with exponential backoff and jitter. If it still fails, skip it and raise a low-priority alert. |
| **Bad message** | Invalid JSON, missing field | Don't retry: it fails the same way every time. Skip it and raise a low-priority alert. |
| **Unknown** (probably a bug) | `TypeError` | Retry 5 times in case it's transient after all. Then **stop** and raise a high-priority alert. Never skip: a bug fails every message, so skipping would silently drop all traffic. |

Skipping is fine for prices because the latest one wins: the next update for the same product
replaces a lost one. In the original system, messages that must never be lost (creating the record
everything else hangs off) stop instead of skipping. That's a one-line change in
[`processor.ts`](src/processor.ts).

### What "stop" means

1. `processMessage` throws a `StopConsumerError` with `retriable: false`.
2. KafkaJS sees `retriable: false`. It doesn't retry the batch or restart the consumer. It commits
   the offsets resolved so far and emits a `CRASH` event with `restart: false`.
3. The app exits with code 1, and Kubernetes restarts it.
4. The failed message's offset was never committed, so the consumer reads it again and stops
   again. The partition stays stuck, and the alert keeps firing, until someone fixes the bug or
   decides to [skip the message](docs/kafka-commands.md#recipe-skip-the-one-message-the-consumer-is-stuck-on).

A stuck partition with a page beats silently corrupt data.

### Other details

- **Offsets are resolved only after the write has finished** ([`batch-handler.ts`](src/batch-handler.ts)).
  That's the fix for the missing `await`. A process-wide `unhandledRejection` handler is the last
  line of defence.
- **Heartbeats during backoff**, so the broker doesn't decide the consumer is dead and rebalance.
- **Idempotent writes.** A price older than the one already saved is ignored, so redeliveries,
  replays and rewinds are harmless.
- **Race-safe writes.** The read-then-write uses a version check (optimistic locking). If another
  writer gets in between, the write fails as transient and the retry starts from a fresh read.
- **The retry budget fits inside the session timeout.** Five retries back off for about 9 seconds
  at most, well under KafkaJS's 30-second session timeout.
- **A KafkaJS gotcha.** Most errors thrown out of `eachBatch` make KafkaJS fetch and run the batch
  again (up to 5 times by default) and then restart the consumer. With its default auto-resolve,
  messages that had already succeeded get processed again. Here `processMessage` handles every
  error itself, so only `StopConsumerError` ever escapes.

## Run it

You need Node 22 and Docker. With Podman, use `podman` in place of `docker`.

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic prices --partitions 3
npm install

npm start                    # terminal 1: the consumer
npm run produce -- good      # terminal 2: send messages
```

Stop the consumer with Ctrl+C. The database is in memory, so it starts empty on every run.

To see topics, messages and consumer lag in a browser, open the Kafka UI at http://localhost:8080.

## Scenarios

The output below is real, with timestamps removed.

### 1. Happy path

```bash
npm run produce -- good
```

```
level=info event=price_saved partition=2 offset=0 productId=kettle price=20
level=info event=price_saved partition=2 offset=1 productId=kettle price=18
level=info event=price_saved partition=1 offset=0 productId=toaster price=21
```

Each product is the message key, so all of a product's updates land on the same partition, in order.

### 2. A bad message is skipped

```bash
npm run produce -- bad
```

```
level=info event=price_saved partition=2 offset=4 productId=kettle price=25
level=error event=message_skipped partition=2 offset=5 kind=poison error="message body is not valid JSON" alert=low
level=info event=price_saved partition=2 offset=6 productId=kettle price=24
```

### 3. A flaky database is absorbed by retries

```bash
FAULT_DB_FAILURE_RATE=0.2 npm start
npm run produce -- many
```

```
level=warn event=retrying partition=1 offset=10 attempt=1 delayMs=37 error="get timed out"
level=warn event=retrying partition=1 offset=10 attempt=2 delayMs=408 error="put timed out"
level=info event=price_saved partition=1 offset=10 productId=product-0 price=14
```

In one run, 1 in 5 database calls failed, and all 100 messages were saved after 54 retries. A
message whose database calls fail six times in a row would be skipped with a low-priority alert.

### 4. The database stays down

```bash
FAULT_DB_FAILURE_RATE=1 npm start
npm run produce -- good
```

```
level=warn event=retrying partition=1 offset=2 attempt=1 delayMs=17 error="get timed out"
...
level=warn event=retrying partition=1 offset=2 attempt=5 delayMs=3436 error="get timed out"
level=error event=message_skipped partition=1 offset=2 kind=transient error="get timed out" alert=low
```

### 5. A bug stops the consumer

```bash
npm run produce -- bug     # a price for product "BUG", which triggers a simulated TypeError
```

```
level=warn event=retrying partition=2 offset=7 attempt=5 delayMs=2027 error="Cannot read properties of undefined (simulated bug in get)"
level=fatal event=consumer_stopping partition=2 offset=7 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=fatal event=consumer_crashed alert=high error="can't process partition 2 offset 7"
level=info event=shutting_down exitCode=1
```

Run `npm start` again and it stops on the same message, which is what Kubernetes would see as a
crash loop. To get going again, inspect the message and skip it with the commands in
[docs/kafka-commands.md](docs/kafka-commands.md#recipe-skip-the-one-message-the-consumer-is-stuck-on).

### 6. An out-of-date price is ignored

Send a price with an old timestamp while the consumer is running:

```bash
echo 'kettle|{"productId":"kettle","price":5,"updatedAt":1}' | docker exec -i kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic prices \
  --property parse.key=true --property "key.separator=|"
```

```
level=info event=price_out_of_date partition=2 offset=4 productId=kettle price=5
```

### 7. A write race

Two writers updating the same price at once is hard to trigger by hand, so it's covered by a test:
"re-reads and retries when another writer gets in between the read and the write" in
[`processor.test.ts`](test/processor.test.ts).

## Kafka commands

[docs/kafka-commands.md](docs/kafka-commands.md) covers the commands you need around a consumer like
this: reading a specific offset, checking lag, sending messages by hand, rewinding a single
partition, skipping a stuck message, and starting over.

## Tests

```bash
npm test
npm run typecheck
```

The tests cover each row of the rule table, the write race, out-of-date prices, and that the batch
handler only resolves an offset after its message has finished.

## Layout

```
src/
  index.ts           wiring: KafkaJS consumer, crash and signal handling
  batch-handler.ts   eachBatch loop: process, then resolve the offset
  processor.ts       the rule: retry, skip or stop
  errors.ts          error types and classify()
  retry.ts           exponential backoff with full jitter
  price.ts           parse a message, save a price unless it's out of date
  store.ts           in-memory "prices" collection with versioning and fault injection
  logger.ts          one logfmt line per event
scripts/produce.ts   sends the demo scenarios
docs/kafka-commands.md
```

## License

MIT
