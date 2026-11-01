import { Kafka, logLevel, Partitioners, type Message } from "kafkajs";

let clock = Date.now();

const SCENARIOS: Record<string, () => Message[]> = {
  // Two price changes for each of three products. All saved.
  good: () => ["kettle", "toaster", "lamp"].flatMap((id, i) => [price(id, 20 + i), price(id, 18 + i)]),

  // A malformed message between two good ones. Skipped with a low alert; the consumer carries on.
  bad: () => [price("kettle", 25), { key: "kettle", value: "{not json" }, price("kettle", 24)],

  // A message that hits a (simulated) bug. The consumer stops with a high alert, and
  // stops again on the same message after every restart until someone deals with it.
  bug: () => [price("BUG", 1)],

  // 100 updates across 10 products. Use with FAULT_DB_FAILURE_RATE, or to have offsets to rewind.
  many: () => Array.from({ length: 100 }, (_, i) => price(`product-${i % 10}`, 10 + (i % 7))),
};

const name = process.argv[2] ?? "good";
const scenario = SCENARIOS[name];
if (!scenario) {
  console.error(`Unknown scenario "${name}". Pick one of: ${Object.keys(SCENARIOS).join(", ")}`);
  process.exit(1);
}

const kafka = new Kafka({ clientId: "demo-producer", brokers: [process.env["KAFKA_BROKERS"] ?? "localhost:9092"], logLevel: logLevel.WARN });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
await producer.connect();
const messages = scenario();
await producer.send({ topic: process.env["KAFKA_TOPIC"] ?? "prices", messages });
await producer.disconnect();
console.log(`Sent ${messages.length} message(s) for "${name}"`);

// Keyed by product, so a product's updates always land on the same partition, in order.
function price(productId: string, value: number): Message {
  return { key: productId, value: JSON.stringify({ productId, price: value, updatedAt: clock++ }) };
}
