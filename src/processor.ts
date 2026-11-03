import type { Breaker } from "./breaker.js";
import type { SendToDeadLetters } from "./dead-letter.js";
import { classify, errorMessage, StopConsumerError } from "./errors.js";
import type { Logger } from "./logger.js";
import { applyPriceUpdate, parsePriceUpdate } from "./price.js";
import { withRetry, type RetryOptions } from "./retry.js";
import type { PriceStore } from "./store.js";

export interface InboundMessage {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
}

export type Outcome = "applied" | "stale" | "dead-lettered";

export interface ProcessorDeps {
  store: PriceStore;
  log: Logger;
  retry: RetryOptions;
  deadLetter: SendToDeadLetters;
  breaker: Breaker;
}

/**
 * Process one message. Resolves once it's safe to commit the offset: the price was
 * saved, was already out of date, or was parked in the DLQ. Throws StopConsumerError
 * when it isn't safe.
 */
export async function processMessage(
  msg: InboundMessage,
  deps: ProcessorDeps,
  heartbeat: () => Promise<void>,
): Promise<Outcome> {
  const where = { partition: msg.partition, offset: msg.offset };

  try {
    const update = parsePriceUpdate(msg.value);
    const result = await withRetry(() => applyPriceUpdate(update, deps.store), {
      ...deps.retry,
      isRetriable: (err) => classify(err) !== "poison",
      onRetry: async (err, attempt, delayMs) => {
        deps.log.warn("retrying", { ...where, attempt, delayMs: Math.round(delayMs), error: errorMessage(err) });
        // Backing off can take seconds, so keep the group membership alive. A failed
        // heartbeat means a rebalance has started; the batch loop notices that after
        // this message, so it's safe to ignore here.
        await heartbeat().catch(() => {});
      },
    });
    deps.breaker.recordSuccess();
    deps.log.info(result === "applied" ? "price_saved" : "price_out_of_date", { ...where, productId: update.productId, price: update.price });
    return result;
  } catch (err) {
    const kind = classify(err);
    const fields = { ...where, kind, error: errorMessage(err) };

    // One message that hits a bug is parked like any other failure. But a bug can fail
    // every message, and parking them all would move all traffic into the DLQ while the
    // consumer looks healthy. So after a run of unknown errors, stop instead.
    if (kind === "unknown" && deps.breaker.recordFailure()) {
      deps.log.fatal("consumer_stopping", { ...fields, unknownInARow: deps.breaker.failuresInARow, alert: "high" });
      throw new StopConsumerError(`can't process partition ${msg.partition} offset ${msg.offset}`, { cause: err });
    }

    // Never move past a message that wasn't parked. Stop, and read it again after the restart.
    try {
      await deps.deadLetter({ msg, kind, error: errorMessage(err) });
    } catch (dlqErr) {
      deps.log.fatal("consumer_stopping", { ...fields, dlqError: errorMessage(dlqErr), alert: "high" });
      throw new StopConsumerError(`can't park partition ${msg.partition} offset ${msg.offset}`, { cause: dlqErr });
    }

    // A bad payload, or a database that stayed down through every retry, can wait in the
    // DLQ. A bug needs someone to fix the code.
    deps.log.error("message_dead_lettered", { ...fields, alert: kind === "unknown" ? "high" : "low" });
    return "dead-lettered";
  }
}
