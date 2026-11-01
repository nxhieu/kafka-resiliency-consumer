import { silentLogger } from "../src/logger.js";
import type { InboundMessage, ProcessorDeps } from "../src/processor.js";
import type { PriceStore } from "../src/store.js";

export function makeDeps(store: PriceStore): ProcessorDeps {
  return { store, log: silentLogger, retry: { retries: 5, initialDelayMs: 0, maxDelayMs: 0 } };
}

export function inbound(body: unknown): InboundMessage {
  const value = typeof body === "string" ? body : JSON.stringify(body);
  return { partition: 0, offset: "42", value: Buffer.from(value) };
}
