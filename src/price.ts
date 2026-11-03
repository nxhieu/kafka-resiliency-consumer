import { PoisonMessageError } from "./errors.js";
import type { Price, PriceStore } from "./store.js";

/** Parse and validate a message body. Anything wrong with it is a PoisonMessageError. */
export function parsePriceUpdate(value: Buffer | null): Price {
  if (!value || value.length === 0) throw new PoisonMessageError("message body is empty");

  let body: unknown;
  try {
    body = JSON.parse(value.toString("utf8"));
  } catch {
    throw new PoisonMessageError("message body is not valid JSON");
  }
  if (typeof body !== "object" || body === null) throw new PoisonMessageError("message body is not a JSON object");

  const { productId, price, updatedAt } = body as Record<string, unknown>;
  if (typeof productId !== "string" || productId === "") throw new PoisonMessageError(`"productId" must be a non-empty string`);
  if (typeof price !== "number" || !Number.isFinite(price) || price < 0) throw new PoisonMessageError(`"price" must be a non-negative number`);
  if (typeof updatedAt !== "number" || !Number.isInteger(updatedAt)) throw new PoisonMessageError(`"updatedAt" must be epoch milliseconds`);
  return { productId, price, updatedAt };
}

/**
 * Save the price unless we already have a newer one. Safe to run more than once: Kafka
 * delivers at least once, failed attempts are retried, partitions get rewound, and
 * parked messages get replayed from the DLQ, in any order.
 */
export async function applyPriceUpdate(update: Price, store: PriceStore): Promise<"applied" | "stale"> {
  const current = await store.get(update.productId);

  // Replays and out-of-order deliveries carry older prices. Ignoring them is what makes
  // reprocessing harmless.
  if (current && current.value.updatedAt >= update.updatedAt) return "stale";

  // Write only if nothing changed since the read. If another writer got in between (a
  // time-of-check to time-of-use race), put() throws a transient error and the retry
  // starts again from a fresh read.
  await store.put(update, current?.version ?? 0);
  return "applied";
}
