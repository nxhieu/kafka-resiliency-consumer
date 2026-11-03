import { Kafka, logLevel, Partitioners, type IHeaders, type KafkaMessage } from "kafkajs";
import { DLQ_HEADERS } from "../src/dead-letter.js";

// Sends parked messages back to the topic they came from, once whatever made them fail
// is fixed. Replays are safe in any order: the consumer keeps the newest price per product
// and ignores older ones.
//
//   npm run replay-dlq                 # kinds unknown and transient (the default)
//   npm run replay-dlq -- unknown      # only the messages that hit a bug
//
// Progress is kept like any consumer's: as the committed offsets of a consumer group,
// price-consumer-dlq-replay. Each run carries on from where the last one stopped, so
// nothing is replayed twice. Kinds not asked for (bad messages, by default) are passed
// over and committed too. To go back to them, reset the group's offsets:
// docs/kafka-commands.md#look-at-the-dlq
//
// A consumer never finishes on its own, so each run stops at the end of the DLQ as it was
// when the run started. A replay that fails again is parked again after that point, and
// waits for the next run.

const env = process.env;
const topic = env["KAFKA_TOPIC"] ?? "prices";
const dlqTopic = env["KAFKA_DLQ_TOPIC"] ?? `${topic}.dlq`;
const groupId = `${env["KAFKA_GROUP_ID"] ?? "price-consumer"}-dlq-replay`;
const kinds = new Set((process.argv[2] ?? "unknown,transient").split(","));

const kafka = new Kafka({ clientId: "dlq-replay", brokers: (env["KAFKA_BROKERS"] ?? "localhost:9092").split(","), logLevel: logLevel.WARN });
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId });
const producer = kafka.producer({ allowAutoTopicCreation: false, createPartitioner: Partitioners.DefaultPartitioner });
const counts = { replayed: new Map<string, number>(), passedOver: new Map<string, number>() };

await admin.connect();
const stopAt = await partitionsToReplay();
await admin.disconnect();

if (stopAt.size === 0) {
  console.log(`Nothing new in ${dlqTopic} since the last replay`);
} else {
  let finished = () => {};
  const done = new Promise<void>((resolve) => (finished = resolve));

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: [dlqTopic], fromBeginning: true });
  await consumer.run({
    // Offsets are resolved by hand, like the main consumer's, so a commit only ever covers
    // messages this run actually handled.
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, uncommittedOffsets, pause, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) return;
        const end = stopAt.get(batch.partition);
        if (end === undefined || Number(message.offset) >= end) break;

        await replay(message);
        resolveOffset(message.offset);
        await heartbeat();
        if (Number(message.offset) + 1 >= end) stopAt.delete(batch.partition);
      }

      // Done with this partition. Stop fetching it, so anything parked after the run
      // started stays unread and uncommitted.
      if (!stopAt.has(batch.partition)) pause();
      if (stopAt.size === 0) {
        await commitOffsetsIfNecessary(uncommittedOffsets());
        finished();
      }
    },
  });

  await done;
  await consumer.disconnect();
  await producer.disconnect();

  console.log(`Replayed ${total(counts.replayed)} message(s) from ${dlqTopic}${describe(counts.replayed)}`);
  if (counts.passedOver.size > 0) console.log(`Passed over ${total(counts.passedOver)}${describe(counts.passedOver)}`);
}

/**
 * Where to stop on each partition that has something this group hasn't replayed yet: the
 * end of the partition right now.
 */
async function partitionsToReplay(): Promise<Map<number, number>> {
  const [committed] = await admin.fetchOffsets({ groupId, topics: [dlqTopic] });
  // -1 means the group has never committed on that partition.
  const nextToRead = new Map(committed?.partitions.map(({ partition, offset }) => [partition, Number(offset)]));
  const ends = await admin.fetchTopicOffsets(dlqTopic);

  // A group that has never run starts from the oldest message still kept (low), like
  // fromBeginning does. Retention may have deleted the ones before it.
  return new Map(
    ends
      .filter(({ partition, low, high }) => Math.max(nextToRead.get(partition) ?? -1, Number(low)) < Number(high))
      .map(({ partition, high }) => [partition, Number(high)]),
  );
}

async function replay(message: KafkaMessage): Promise<void> {
  const kind = header(message.headers, DLQ_HEADERS.kind) ?? "missing";
  const originalTopic = header(message.headers, DLQ_HEADERS.topic);
  if (originalTopic && kinds.has(kind)) {
    await producer.send({ topic: originalTopic, messages: [{ key: message.key, value: message.value }] });
    increment(counts.replayed, kind);
  } else {
    increment(counts.passedOver, kind);
  }
}

function header(headers: IHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];
  return value === undefined || Array.isArray(value) ? undefined : value.toString();
}

function increment(byKind: Map<string, number>, kind: string): void {
  byKind.set(kind, (byKind.get(kind) ?? 0) + 1);
}

function total(byKind: Map<string, number>): number {
  return [...byKind.values()].reduce((sum, n) => sum + n, 0);
}

function describe(byKind: Map<string, number>): string {
  return byKind.size === 0 ? "" : ` (${[...byKind].map(([kind, n]) => `${kind}: ${n}`).join(", ")})`;
}
