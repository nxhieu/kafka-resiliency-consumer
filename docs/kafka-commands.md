# Kafka commands

A hands-on guide to the Kafka CLI, using the local broker from `docker-compose.yml` and this
project's `prices` topic and `price-consumer` group. Every command here was run against it, and
the outputs are real (trimmed for width).

## Setup

The CLI tools live inside the container at `/opt/kafka/bin`. This shell function saves typing
(works in bash and zsh; with Podman, change `docker` to `podman`):

```bash
k() { docker exec -i kafka "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}"; }
```

`k kafka-topics.sh --list` then runs
`docker exec -i kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`.

## Five ideas that explain every command below

1. A **topic** is split into **partitions**. Messages with the same **key** always go to the same
   partition, so all of one product's prices stay in order.
2. An **offset** is a message's position inside its partition. Offsets are per partition:
   partition 1 offset 5 and partition 2 offset 5 are different messages.
3. A **consumer group** stores one **committed offset** per partition: the next message it will
   read. `LAG = LOG-END-OFFSET - CURRENT-OFFSET` is how far behind it is.
4. **Rewinding** or **skipping** means changing a group's committed offsets. Kafka only allows it
   while no consumer in the group is running.
5. Changing offsets doesn't touch the messages. They stay in the topic until retention deletes them.

## Topics

```bash
k kafka-topics.sh --list
k kafka-topics.sh --create --topic prices --partitions 3
k kafka-topics.sh --create --topic prices.dlq --partitions 3
k kafka-topics.sh --describe --topic prices
```

```
Topic: prices  PartitionCount: 3  ReplicationFactor: 1
  Topic: prices  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: prices  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: prices  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

## Look at messages

Read everything, showing where each message lives. `--timeout-ms` makes it exit once it's caught
up, instead of waiting forever:

```bash
k kafka-console-consumer.sh --topic prices --from-beginning --timeout-ms 5000 \
  --property print.partition=true --property print.offset=true --property print.key=true
```

```
Partition:1  Offset:0  toaster  {"productId":"toaster","price":21,"updatedAt":1793535648621}
Partition:1  Offset:1  toaster  {"productId":"toaster","price":19,"updatedAt":1793535648622}
```

Read specific messages. This is how you inspect the message a stopped consumer is stuck on:

```bash
k kafka-console-consumer.sh --topic prices --partition 2 --offset 4 --max-messages 3 \
  --property print.offset=true --property print.key=true
```

```
Offset:4  kettle  {"productId":"kettle","price":25,"updatedAt":1793535324813}
Offset:5  kettle  {not json
Offset:6  kettle  {"productId":"kettle","price":24,"updatedAt":1793535324814}
```

The first and last offset of each partition:

```bash
k kafka-get-offsets.sh --topic prices              # end offsets (the next offset to be written)
k kafka-get-offsets.sh --topic prices --time -2    # earliest offsets still kept
k kafka-get-offsets.sh --topic prices --partitions 2
```

```
prices:0:60
prices:1:12
prices:2:38
```

## Look at the DLQ

A parked message keeps its original key and value. Where it came from and why it failed are in
headers, so print those too:

```bash
k kafka-console-consumer.sh --topic prices.dlq --partition 2 --offset 0 --max-messages 2 \
  --property print.offset=true --property print.key=true --property print.headers=true
```

```
Offset:0  dlq.original.topic:prices,dlq.original.partition:2,dlq.original.offset:5,dlq.error.kind:poison,dlq.error.message:message body is not valid JSON,dlq.failed.at:2026-11-03T03:33:24.164Z  kettle  {not json
Offset:1  dlq.original.topic:prices,dlq.original.partition:2,dlq.original.offset:7,dlq.error.kind:transient,dlq.error.message:get timed out,dlq.failed.at:2026-11-03T03:33:44.494Z  kettle  {"productId":"kettle","price":20,"updatedAt":1793676818061}
```

The key is kept, so a product's parked messages land on one DLQ partition, in the order they
were parked. `dlq.original.partition` and `dlq.original.offset` point at the original, which is
still in `prices` until retention deletes it.

### Replay it

Once the cause is fixed, `npm run replay-dlq` sends parked messages back to `prices` (see
[scenario 7](../README.md#7-replay-after-the-fix)). It replays `unknown` and `transient` messages
by default. Bad messages (`poison`) fail the same way every time, so it passes over them.

The replay is an ordinary consumer group, `price-consumer-dlq-replay`, and keeps its place with
committed offsets. Its lag is the number of parked messages nobody has replayed yet:

```bash
k kafka-consumer-groups.sh --describe --group price-consumer-dlq-replay
```

```
GROUP                      TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
price-consumer-dlq-replay  prices.dlq  0          0               0               0
price-consumer-dlq-replay  prices.dlq  1          2               2               0
price-consumer-dlq-replay  prices.dlq  2          13              13              0
```

### Recipe: replay the bad messages after all

Passed-over messages are committed too, so the next replay won't see them. If a parser fix means
they'd now be accepted, rewind the replay group and ask for `poison` only:

```bash
k kafka-consumer-groups.sh --reset-offsets --group price-consumer-dlq-replay --topic prices.dlq --to-earliest --execute
npm run replay-dlq -- poison
```

```
Replayed 1 message(s) from prices.dlq (poison: 1)
Passed over 14 (transient: 6, unknown: 8)
```

Only the replay group moves. The DLQ's messages and the consumer's own offsets don't change.

## Send messages by hand

`parse.key=true` reads `key|value` from each line. The `|` separator is used because JSON is full
of `:`.

```bash
echo 'kettle|{"productId":"kettle","price":5,"updatedAt":1}' | \
  k kafka-console-producer.sh --topic prices --property parse.key=true --property "key.separator=|"
```

To type messages interactively instead, use `docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh ...`
with the same options, enter one message per line, and press Ctrl+D to finish.

Same key, same partition: this message lands on partition 2, like every other `kettle` message,
whether it was sent from the CLI or from KafkaJS. With the consumer running and a newer kettle
price already saved, it logs:

```
level=info event=price_out_of_date partition=2 offset=4 productId=kettle price=5
```

## Where is the consumer group?

```bash
k kafka-consumer-groups.sh --list
k kafka-consumer-groups.sh --describe --group price-consumer            # offsets and lag
k kafka-consumer-groups.sh --describe --group price-consumer --members  # who's connected
k kafka-consumer-groups.sh --describe --group price-consumer --state    # Stable, Empty, rebalancing...
```

After the consumer stops, the lag shows exactly where it's stuck:

```
Consumer group 'price-consumer' has no active members.

GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
price-consumer  prices  1          2               2               0
price-consumer  prices  2          7               8               1
```

Partition 2's committed offset is 7, so offset 7 is the next message it will read: the one it
stopped on. A partition with no committed offset yet (nothing consumed from it) isn't listed.

## Rewind or skip: reset offsets

Rules:

- **Stop the consumer first.** Otherwise:
  ```
  Error: Assignments can only be reset if the group 'price-consumer' is inactive, but the current state is Stable.
  ```
- **Run with `--dry-run` first**, check the NEW-OFFSET column, then run the same command with `--execute`.
- Choose partitions with `--topic`: `prices` (all), `prices:2` (one), `prices:1,2` (several).

| Option | Moves the committed offset to |
|---|---|
| `--to-earliest` | the oldest message still kept (replay everything) |
| `--to-latest` | the end (skip everything not yet read) |
| `--to-offset 3` | offset 3 |
| `--shift-by -3` / `--shift-by 1` | 3 back / 1 forward from where it is now |
| `--by-duration PT10M` | the first message from 10 minutes ago |
| `--to-datetime 2026-11-01T12:15:20.000` | the first message at or after that time (the container's clock is UTC) |

### Recipe: skip the one message the consumer is stuck on

This is what you do after a [stop](../README.md#6-a-broken-deploy-stops-the-consumer), once
you've decided the message can be dropped rather than fixed and processed:

```bash
k kafka-consumer-groups.sh --reset-offsets --group price-consumer --topic prices:2 --shift-by 1 --dry-run
k kafka-consumer-groups.sh --reset-offsets --group price-consumer --topic prices:2 --shift-by 1 --execute
```

```
GROUP           TOPIC   PARTITION  NEW-OFFSET
price-consumer  prices  2          8
```

### Recipe: replay one partition from the start

```bash
k kafka-consumer-groups.sh --reset-offsets --group price-consumer --topic prices:1 --to-offset 0 --execute
npm start
```

```
level=info event=price_saved partition=1 offset=0 productId=toaster price=21
level=info event=price_saved partition=1 offset=1 productId=toaster price=19
```

Only partition 1 is replayed. The others carry on from where they were.

### Recipe: replay the last 10 minutes on every partition

```bash
k kafka-consumer-groups.sh --reset-offsets --group price-consumer --topic prices --by-duration PT10M --dry-run
```

### Things that catch people out

- **A rewind replays bad messages too.** Rewinding partition 2 to before a parked message
  processes it again, and parks it again: the DLQ ends up with two copies.
- **Replays are only safe if processing is idempotent.** Here an out-of-date price is ignored, so
  replaying old messages can't overwrite newer data.
- **"Stable" after you've stopped the consumer** means it didn't leave the group cleanly (it was
  killed with `kill -9`, or crashed hard). The broker drops it after the session timeout (30
  seconds for KafkaJS), and then the reset works. A Ctrl+C on `npm start` leaves cleanly.
- `--to-latest` throws away the backlog for good, as far as this group is concerned.

## Start over

```bash
k kafka-consumer-groups.sh --delete --group price-consumer   # the consumer starts from the beginning again
k kafka-topics.sh --delete --topic prices                    # throws away the messages too
k kafka-topics.sh --delete --topic prices.dlq                # and the parked ones
k kafka-topics.sh --create --topic prices --partitions 3
k kafka-topics.sh --create --topic prices.dlq --partitions 3
```

Deleting the group means the next run reads the whole topic again (this consumer subscribes with
`fromBeginning: true`), bad messages included, and parks them again.

## On a real cluster

The commands are the same. Point `--bootstrap-server` at the real brokers and pass the
authentication settings in a properties file with `--command-config client.properties`
(`--consumer.config` / `--producer.config` for the console tools). Always `--dry-run` a reset first.
