import { Kafka, logLevel } from "kafkajs";
import { createBatchHandler } from "./batch-handler.js";
import { errorMessage } from "./errors.js";
import { createLogger } from "./logger.js";
import { processMessage } from "./processor.js";
import { InMemoryPriceStore } from "./store.js";

const env = process.env;
const brokers = (env["KAFKA_BROKERS"] ?? "localhost:9092").split(",");
const topic = env["KAFKA_TOPIC"] ?? "prices";
const groupId = env["KAFKA_GROUP_ID"] ?? "price-consumer";
const retry = { retries: 5, initialDelayMs: 300, maxDelayMs: 5000 };
const failureRate = Number(env["FAULT_DB_FAILURE_RATE"] ?? 0);

const log = createLogger();
const store = new InMemoryPriceStore({ failureRate, bugProductId: "BUG" });
const kafka = new Kafka({ clientId: "kafka-resiliency-consumer", brokers, logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId });

let shuttingDown = false;

async function shutdown(exitCode: number): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info("shutting_down", { exitCode });
  await consumer.disconnect().catch((err: unknown) => log.error("disconnect_failed", { error: errorMessage(err) }));
  process.exit(exitCode);
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
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  await consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: createBatchHandler((msg, heartbeat) => processMessage(msg, { store, log, retry }, heartbeat)),
  });
  log.info("consumer_started", { topic, groupId, failureRate });
} catch (err) {
  log.fatal("startup_failed", { alert: "high", error: errorMessage(err) });
  await shutdown(1);
}
