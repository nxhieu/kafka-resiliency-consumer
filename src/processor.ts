import { classify, errorMessage, StopConsumerError } from "./errors.js";
import type { Logger } from "./logger.js";
import { applyPriceUpdate, parsePriceUpdate } from "./price.js";
import { withRetry, type RetryOptions } from "./retry.js";
import type { PriceStore } from "./store.js";

export interface InboundMessage {
  partition: number;
  offset: string;
  value: Buffer | null;
}

export type Outcome = "applied" | "stale" | "skipped";

export interface ProcessorDeps {
  store: PriceStore;
  log: Logger;
  retry: RetryOptions;
}

/**
 * Process one message. Resolves once it's safe to commit the offset: the price was
 * saved, was already out of date, or was deliberately skipped. Throws StopConsumerError
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
    deps.log.info(result === "applied" ? "price_saved" : "price_out_of_date", { ...where, productId: update.productId, price: update.price });
    return result;
  } catch (err) {
    const kind = classify(err);
    const fields = { ...where, kind, error: errorMessage(err) };

    // A bug would fail every message, and skipping them all would quietly drop all
    // traffic. So an unknown error stops the consumer.
    if (kind === "unknown") {
      deps.log.fatal("consumer_stopping", { ...fields, alert: "high" });
      throw new StopConsumerError(`can't process partition ${msg.partition} offset ${msg.offset}`, { cause: err });
    }

    // A bad payload, or a database that stayed down through every retry. Losing one
    // price is cheap, because the next update for the same product replaces it.
    deps.log.error("message_skipped", { ...fields, alert: "low" });
    return "skipped";
  }
}
