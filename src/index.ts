import { Kafka, logLevel, Partitioners } from "kafkajs";
import { createBatchHandler } from "./batch-handler.js";
import { Breaker } from "./breaker.js";
import { kafkaDeadLetters } from "./dead-letter.js";
import { errorMessage } from "./errors.js";
import { createLogger } from "./logger.js";
import { processMessage } from "./processor.js";
import { InMemoryPriceStore } from "./store.js";

const env = process.env;
const brokers = (env["KAFKA_BROKERS"] ?? "localhost:9092").split(",");
const topic = env["KAFKA_TOPIC"] ?? "prices";
const dlqTopic = env["KAFKA_DLQ_TOPIC"] ?? `${topic}.dlq`;
const groupId = env["KAFKA_GROUP_ID"] ?? "price-consumer";
const retry = { retries: 5, initialDelayMs: 300, maxDelayMs: 5000 };
// After this many unknown errors in a row, stop instead of parking another one.
const breaker = new Breaker(5);
const failureRate = Number(env["FAULT_DB_FAILURE_RATE"] ?? 0);
const bugRate = Number(env["FAULT_BUG_RATE"] ?? 0);

const log = createLogger();
const store = new InMemoryPriceStore({ failureRate, bugRate, bugProductId: "BUG" });
const kafka = new Kafka({ clientId: "kafka-resiliency-consumer", brokers, logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId });
// A missing DLQ topic should fail the send, not get created with the broker's default
// partition count.
const producer = kafka.producer({ allowAutoTopicCreation: false, createPartitioner: Partitioners.DefaultPartitioner });
const deadLetter = kafkaDeadLetters(producer, dlqTopic);

let shuttingDown = false;

async function shutdown(exitCode: number): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info("shutting_down", { exitCode });
  // The consumer first: it waits for the batch in flight, which may still be parking a message.
  await consumer.disconnect().catch((err: unknown) => log.error("disconnect_failed", { error: errorMessage(err) }));
  await producer.disconnect().catch((err: unknown) => log.error("disconnect_failed", { error: errorMessage(err) }));
  process.exit(exitCode);
}

async function requireTopic(name: string): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    if (!(await admin.listTopics()).includes(name)) throw new Error(`topic ${name} doesn't exist; create it before starting`);
  } finally {
    await admin.disconnect();
  }
}

// KafkaJS restarts the consumer by itself after a retriable crash. When it won't (a
// StopConsumerError, or a programming error such as a TypeError), exit non-zero and let
// the orchestrator restart the process, with its own backoff.
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (payload.restart) {
    log.warn("consumer_restarting", { error: errorMessage(payload.error) });
    return;
  }
  log.fatal("consumer_crashed", { alert: "high", error: errorMessage(payload.error) });
  void shutdown(1);
});

process.on("SIGTERM", () => void shutdown(0));
process.on("SIGINT", () => void shutdown(0));

// Last line of defence against a forgotten await: fail loudly instead of carrying on in
// an unknown state.
process.on("unhandledRejection", (reason) => {
  log.fatal("unhandled_rejection", { alert: "high", error: errorMessage(reason) });
  void shutdown(1);
});

try {
  // Without somewhere to park messages, the consumer could only stop or drop them.
  await requireTopic(dlqTopic);
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  await consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: createBatchHandler((msg, heartbeat) => processMessage(msg, { store, log, retry, deadLetter, breaker }, heartbeat)),
  });
  log.info("consumer_started", { topic, dlqTopic, groupId, failureRate, bugRate });
} catch (err) {
  log.fatal("startup_failed", { alert: "high", error: errorMessage(err) });
  await shutdown(1);
}
