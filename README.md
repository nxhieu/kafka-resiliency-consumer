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
| **Transient** | Database timeout, dropped connection, write conflict | Retry up to 5 times with exponential backoff and jitter. If it still fails, park it in the DLQ and raise a low-priority alert. |
| **Bad message** | Invalid JSON, missing field | Don't retry: it fails the same way every time. Park it in the DLQ and raise a low-priority alert. |
| **Unknown** (probably a bug) | `TypeError` | Retry 5 times in case it's transient after all. Then park it in the DLQ and raise a high-priority alert: a bug needs a fix. |
| **Unknown, 5 in a row** | A deploy that broke every message | **Stop** and raise a high-priority alert. |

Parking means sending the message to a dead-letter topic, `prices.dlq`, with its original key and
value, and headers saying where it came from and why it failed ([`dead-letter.ts`](src/dead-letter.ts)).
Nothing is dropped. Once the cause is fixed, `npm run replay-dlq` sends parked messages back.

### Why stop at all?

A bug might only affect one product, and stopping would hold up every other product on the same
partition. So one message that hits a bug is parked, and the consumer moves on.

But a bug can just as easily fail every message. Parking all of them would move all traffic into
the DLQ while the consumer shows zero lag and looks healthy. From a single failure the consumer
can't tell the two apart, so it counts: 5 unknown errors with no saved price in between trips a
breaker ([`breaker.ts`](src/breaker.ts)), and the consumer stops on the fifth instead of parking it.

Only a saved price resets the count, because only a saved price shows the code works. A bad
message or a database outage neither counts nor resets it.

### Why parking doesn't break ordering here

Kafka keeps a product's prices in order on one partition. Parking one and carrying on means the
next price is processed out of turn, and a replay delivers the parked one late.

That's harmless here, because the consumer doesn't rely on order. It keeps the price with the
newest `updatedAt` and ignores older ones ([`price.ts`](src/price.ts)). A replayed price that has
been overtaken is ignored, and one that's still the newest is saved. Replays can run in any order,
and more than once.

That isn't true of every topic. If messages build on each other (create a record, then update it),
processing the update after parking the create is wrong. There you'd park every later message for
the same key too, until the first one is dealt with, or stop instead of parking. In
[`processor.ts`](src/processor.ts) that's a one-line change.

### What "stop" means

The consumer stops when the breaker trips, or when it can't write to the DLQ. It never moves past
a message that wasn't parked.

1. `processMessage` throws a `StopConsumerError` with `retriable: false`.
2. KafkaJS sees `retriable: false`. It doesn't retry the batch or restart the consumer. It commits
   the offsets resolved so far and emits a `CRASH` event with `restart: false`.
3. The app exits with code 1, and Kubernetes restarts it.
4. The failed message's offset was never committed, so the consumer reads it again.

The breaker's count lives in memory, so it starts from zero after the restart. A deploy that's
still broken parks up to 4 more messages, then stops again. Kubernetes' growing restart backoff
keeps that slow, the alert keeps firing, and every parked message can be replayed after the fix.
Holding the stop across restarts would need the count stored outside the process. To get going
without a fix, [skip the message](docs/kafka-commands.md#recipe-skip-the-one-message-the-consumer-is-stuck-on).

A DLQ only helps if someone looks at it. Alert on messages arriving in it, not on lag.

### Other details

- **Offsets are resolved only after the write has finished** ([`batch-handler.ts`](src/batch-handler.ts)).
  That's the fix for the missing `await`. A process-wide `unhandledRejection` handler is the last
  line of defence.
- **Heartbeats during backoff**, so the broker doesn't decide the consumer is dead and rebalance.
- **Idempotent writes.** A price older than the one already saved is ignored, so redeliveries,
  rewinds and replays from the DLQ are harmless.
- **Race-safe writes.** The read-then-write uses a version check (optimistic locking). If another
  writer gets in between, the write fails as transient and the retry starts from a fresh read.
- **The retry budget fits inside the session timeout.** Five retries back off for about 9 seconds
  at most, well under KafkaJS's 30-second session timeout.
- **Parking is at least once.** If the consumer dies after parking a message but before committing
  its offset, the message is parked again after the restart. Replaying both is harmless.
- **The DLQ topic must exist before the consumer starts.** It's checked at startup, and the producer
  isn't allowed to create topics, so a typo fails loudly instead of creating a topic with the
  broker's default settings.
- **A crash the code never sees can't be parked.** A message that kills the process (by running
  it out of memory, say) never reaches the `catch`, so it crash-loops instead. Parking it would
  need a count of attempts kept outside the process.
- **A KafkaJS gotcha.** Most errors thrown out of `eachBatch` make KafkaJS fetch and run the batch
  again (up to 5 times by default) and then restart the consumer. With its default auto-resolve,
  messages that had already succeeded get processed again. Here `processMessage` handles every
  error itself, so only `StopConsumerError` ever escapes.

## Run it

You need Node 22 and Docker. With Podman, use `podman` in place of `docker`.

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic prices --partitions 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic prices.dlq --partitions 3
npm install

npm start                    # terminal 1: the consumer
npm run produce -- good      # terminal 2: send messages
```

Stop the consumer with Ctrl+C. The database is in memory, so it starts empty on every run.

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

### 2. A bad message is parked

```bash
npm run produce -- bad
```

```
level=info event=price_saved partition=2 offset=4 productId=kettle price=25
level=error event=message_dead_lettered partition=2 offset=5 kind=poison error="message body is not valid JSON" alert=low
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
message whose database calls fail six times in a row would be parked with a low-priority alert.

### 4. The database stays down

```bash
FAULT_DB_FAILURE_RATE=1 npm start
npm run produce -- good
```

```
level=warn event=retrying partition=2 offset=7 attempt=1 delayMs=289 error="get timed out"
...
level=warn event=retrying partition=2 offset=7 attempt=5 delayMs=4151 error="get timed out"
level=error event=message_dead_lettered partition=2 offset=7 kind=transient error="get timed out" alert=low
```

Every message is parked, and can be replayed once the database is back ([scenario 7](#7-replay-after-the-fix)).

### 5. A message that hits a bug is parked

```bash
npm run produce -- bug     # a price for product "BUG", which triggers a simulated TypeError
npm run produce -- good
```

```
level=warn event=retrying partition=2 offset=11 attempt=1 delayMs=266 error="Cannot read properties of undefined (simulated bug in get)"
...
level=warn event=retrying partition=2 offset=11 attempt=5 delayMs=438 error="Cannot read properties of undefined (simulated bug in get)"
level=error event=message_dead_lettered partition=2 offset=11 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=info event=price_saved partition=1 offset=4 productId=toaster price=21
level=info event=price_saved partition=1 offset=5 productId=toaster price=19
level=info event=price_saved partition=2 offset=12 productId=kettle price=20
level=info event=price_saved partition=2 offset=13 productId=kettle price=18
```

Kettle shares partition 2 with BUG. Stopping here would have held kettle up too.

### 6. A broken deploy stops the consumer

```bash
FAULT_BUG_RATE=1 npm start     # every database call throws a TypeError
npm run produce -- good
```

```
level=error event=message_dead_lettered partition=1 offset=6 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=error event=message_dead_lettered partition=1 offset=7 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=error event=message_dead_lettered partition=2 offset=16 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=error event=message_dead_lettered partition=2 offset=17 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
level=warn event=retrying partition=2 offset=18 attempt=5 delayMs=259 error="Cannot read properties of undefined (simulated bug in get)"
level=fatal event=consumer_stopping partition=2 offset=18 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" unknownInARow=5 alert=high
level=fatal event=consumer_crashed alert=high error="can't process partition 2 offset 18"
level=info event=shutting_down exitCode=1
```

(Retry lines and KafkaJS's own error lines trimmed.) Four messages are parked, and the consumer stops on the fifth. Partition 2's
committed offset stays at 18, so the [lag](docs/kafka-commands.md#where-is-the-consumer-group)
shows where it stopped.

### 7. Replay after the fix

Start the consumer without the fault, as if the fix had been deployed. It carries on from the
message it stopped on. Then send the parked messages back:

```bash
npm start
npm run replay-dlq
```

```
Replayed 11 message(s) from prices.dlq (transient: 6, unknown: 5)
Left 1 in the DLQ (poison: 1)
```

The consumer, meanwhile:

```
...
level=info event=price_saved partition=2 offset=20 productId=kettle price=20
level=info event=price_saved partition=2 offset=21 productId=kettle price=18
level=info event=price_out_of_date partition=2 offset=22 productId=lamp price=22
level=info event=price_out_of_date partition=2 offset=23 productId=lamp price=20
...
level=error event=message_dead_lettered partition=2 offset=24 kind=unknown error="Cannot read properties of undefined (simulated bug in get)" alert=high
```

- The kettle prices parked while the database was down are saved: nothing newer had arrived.
- The lamp prices from the same outage are ignored: newer lamp prices were saved before the
  replay. Replay order doesn't matter.
- The BUG price is parked again, because its simulated bug is never fixed. The replay stops at
  the end of the DLQ as it was when the replay started, so it doesn't loop.
- The bad message is left alone. Replaying can't fix it, so by default only `unknown` and
  `transient` are replayed. `npm run replay-dlq -- poison` would send it back anyway, for example
  after a parser fix.

### 8. An out-of-date price is ignored

Send a price with an old timestamp while the consumer is running:

```bash
echo 'kettle|{"productId":"kettle","price":5,"updatedAt":1}' | docker exec -i kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic prices \
  --property parse.key=true --property "key.separator=|"
```

```
level=info event=price_out_of_date partition=2 offset=4 productId=kettle price=5
```

### 9. A write race

Two writers updating the same price at once is hard to trigger by hand, so it's covered by a test:
"re-reads and retries when another writer gets in between the read and the write" in
[`processor.test.ts`](test/processor.test.ts).

## Kafka commands

[docs/kafka-commands.md](docs/kafka-commands.md) covers the commands you need around a consumer like
this: reading a specific offset, looking at the DLQ, checking lag, sending messages by hand,
rewinding a single partition, skipping a stuck message, and starting over.

## Tests

```bash
npm test
npm run typecheck
```

The tests cover each row of the rule table, the breaker, a DLQ that can't be written, the format
of a parked message, the write race, out-of-date prices, and that the batch handler only resolves
an offset after its message has finished.

## Layout

```
src/
  index.ts             wiring: KafkaJS consumer and DLQ producer, crash and signal handling
  batch-handler.ts     eachBatch loop: process, then resolve the offset
  processor.ts         the rule: retry, park or stop
  breaker.ts           counts unknown errors in a row
  dead-letter.ts       what a parked message looks like, and sending it
  errors.ts            error types and classify()
  retry.ts             exponential backoff with full jitter
  price.ts             parse a message, save a price unless it's out of date
  store.ts             in-memory "prices" collection with versioning and fault injection
  logger.ts            one logfmt line per event
scripts/produce.ts     sends the demo scenarios
scripts/replay-dlq.ts  sends parked messages back to their topic
docs/kafka-commands.md
```

## License

MIT
