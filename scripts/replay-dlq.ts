import { Kafka, logLevel, Partitioners, type IHeaders } from "kafkajs";
import { DLQ_HEADERS } from "../src/dead-letter.js";

// Sends parked messages back to the topic they came from, once whatever made them fail
// is fixed. Replays are safe in any order and any number of times: the consumer keeps the
// newest price per product and ignores older ones.
//
//   npm run replay-dlq                 # kinds unknown and transient (the default)
//   npm run replay-dlq -- unknown      # only the messages that hit a bug
//
// Bad messages (kind poison) fail the same way every time, so they're only replayed if
// you ask for them. Look at them first: docs/kafka-commands.md#look-at-the-dlq
//
// Each run reads the whole DLQ with a throwaway consumer group, and stops at the end
// offsets it saw when it started, so a message parked again during the run isn't replayed
// in a loop.

const env = process.env;
const topic = env["KAFKA_TOPIC"] ?? "prices";
const dlqTopic = env["KAFKA_DLQ_TOPIC"] ?? `${topic}.dlq`;
const kinds = new Set((process.argv[2] ?? "unknown,transient").split(","));
const groupId = `dlq-replay-${Date.now()}`;

const kafka = new Kafka({ clientId: "dlq-replay", brokers: (env["KAFKA_BROKERS"] ?? "localhost:9092").split(","), logLevel: logLevel.WARN });
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId });
const producer = kafka.producer({ allowAutoTopicCreation: false, createPartitioner: Partitioners.DefaultPartitioner });

await admin.connect();
// The next offset to be written, per partition that has anything in it.
const remaining = new Map(
  (await admin.fetchTopicOffsets(dlqTopic)).filter(({ low, high }) => Number(high) > Number(low)).map(({ partition, high }) => [partition, Number(high)]),
);
const counts = { replayed: new Map<string, number>(), passedOver: new Map<string, number>() };

if (remaining.size > 0) {
  let finished = () => {};
  const done = new Promise<void>((resolve) => (finished = resolve));

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: [dlqTopic], fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const end = remaining.get(partition);
      if (end === undefined || Number(message.offset) >= end) return;

      const kind = header(message.headers, DLQ_HEADERS.kind) ?? "missing";
      const originalTopic = header(message.headers, DLQ_HEADERS.topic);
      if (originalTopic && kinds.has(kind)) {
        await producer.send({ topic: originalTopic, messages: [{ key: message.key, value: message.value }] });
        increment(counts.replayed, kind);
      } else {
        increment(counts.passedOver, kind);
      }

      if (Number(message.offset) + 1 >= end) remaining.delete(partition);
      if (remaining.size === 0) finished();
    },
  });

  await done;
  await consumer.disconnect();
  await producer.disconnect();
  await admin.deleteGroups([groupId]);
}
await admin.disconnect();

console.log(`Replayed ${total(counts.replayed)} message(s) from ${dlqTopic}${describe(counts.replayed)}`);
if (counts.passedOver.size > 0) console.log(`Left ${total(counts.passedOver)} in the DLQ${describe(counts.passedOver)}`);

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
