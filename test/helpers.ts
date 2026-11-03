import { Breaker } from "../src/breaker.js";
import type { DeadLetter } from "../src/dead-letter.js";
import { silentLogger } from "../src/logger.js";
import type { InboundMessage, ProcessorDeps } from "../src/processor.js";
import type { PriceStore } from "../src/store.js";

/** No retry delays, and a DLQ that records what it was sent in `parked`. */
export function makeDeps(store: PriceStore, overrides: Partial<ProcessorDeps> = {}): ProcessorDeps & { parked: DeadLetter[] } {
  const parked: DeadLetter[] = [];
  return {
    store,
    log: silentLogger,
    retry: { retries: 5, initialDelayMs: 0, maxDelayMs: 0 },
    deadLetter: async (letter) => {
      parked.push(letter);
    },
    breaker: new Breaker(5),
    ...overrides,
    parked,
  };
}

export function inbound(body: unknown, offset = "42"): InboundMessage {
  const value = typeof body === "string" ? body : JSON.stringify(body);
  return { topic: "prices", partition: 0, offset, key: Buffer.from("kettle"), value: Buffer.from(value) };
}
