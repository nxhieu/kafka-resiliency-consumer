import type { Message, Producer } from "kafkajs";
import type { ErrorKind } from "./errors.js";
import type { InboundMessage } from "./processor.js";

/** A message the consumer gave up on, and why. */
export interface DeadLetter {
  msg: InboundMessage;
  kind: ErrorKind;
  error: string;
}

export type SendToDeadLetters = (letter: DeadLetter) => Promise<void>;

export const DLQ_HEADERS = {
  topic: "dlq.original.topic",
  partition: "dlq.original.partition",
  offset: "dlq.original.offset",
  kind: "dlq.error.kind",
  error: "dlq.error.message",
  failedAt: "dlq.failed.at",
} as const;

/**
 * The key and value are copied byte for byte, so replaying a parked message is a plain
 * copy back to its topic. Where it came from and why it failed go in headers.
 *
 * Keeping the key keeps a product's parked messages on one DLQ partition, in order.
 */
export function toDeadLetterMessage({ msg, kind, error }: DeadLetter, now: Date = new Date()): Message {
  return {
    key: msg.key,
    value: msg.value,
    headers: {
      [DLQ_HEADERS.topic]: msg.topic,
      [DLQ_HEADERS.partition]: String(msg.partition),
      [DLQ_HEADERS.offset]: msg.offset,
      [DLQ_HEADERS.kind]: kind,
      [DLQ_HEADERS.error]: error,
      [DLQ_HEADERS.failedAt]: now.toISOString(),
    },
  };
}

/** Park messages in a Kafka topic. Resolves once the broker has the message. */
export function kafkaDeadLetters(producer: Producer, topic: string): SendToDeadLetters {
  return async (letter) => {
    await producer.send({ topic, messages: [toDeadLetterMessage(letter)] });
  };
}
