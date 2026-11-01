/** Might work if we try again: timeouts, dropped connections, write conflicts. */
export class TransientError extends Error {
  name = "TransientError";
}

/** The message itself is bad. Retrying gives the same result every time. */
export class PoisonMessageError extends Error {
  name = "PoisonMessageError";
}

/**
 * Thrown out of eachBatch to stop consuming. KafkaJS reads `retriable: false` as
 * "don't retry the batch and don't restart the consumer", so the process can exit
 * and let the orchestrator restart it.
 */
export class StopConsumerError extends Error {
  name = "StopConsumerError";
  readonly retriable = false;
}

export type ErrorKind = "transient" | "poison" | "unknown";

const TRANSIENT_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EPIPE", "EAI_AGAIN"]);

export function classify(err: unknown): ErrorKind {
  if (err instanceof PoisonMessageError) return "poison";
  if (err instanceof TransientError) return "transient";
  if (err instanceof Error && "code" in err && TRANSIENT_CODES.has(String(err.code))) return "transient";
  // Anything else is probably a bug. It's retried in case it's a transient error we
  // haven't classified yet, but it's never skipped.
  return "unknown";
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
