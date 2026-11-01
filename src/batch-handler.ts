import type { EachBatchPayload } from "kafkajs";
import type { InboundMessage, Outcome } from "./processor.js";

export type ProcessOne = (msg: InboundMessage, heartbeat: () => Promise<void>) => Promise<Outcome>;

/**
 * KafkaJS eachBatch handler. Each offset is resolved only after its message's work has
 * finished, so a commit never covers a write that's still in flight or that failed.
 */
export function createBatchHandler(processOne: ProcessOne) {
  return async ({ batch, resolveOffset, heartbeat, isRunning, isStale }: EachBatchPayload): Promise<void> => {
    for (const { offset, value } of batch.messages) {
      if (!isRunning() || isStale()) return;

      // A StopConsumerError propagates from here. KafkaJS commits the offsets resolved
      // so far, and this message is read again after the restart.
      await processOne({ partition: batch.partition, offset, value }, heartbeat);
      resolveOffset(offset);
      await heartbeat();
    }
  };
}
