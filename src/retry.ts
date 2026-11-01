import { setTimeout as sleep } from "node:timers/promises";

export interface RetryOptions {
  /** Attempts after the first one. */
  retries: number;
  initialDelayMs: number;
  maxDelayMs: number;
}

export interface RetryHooks {
  isRetriable: (err: unknown) => boolean;
  onRetry?: (err: unknown, attempt: number, delayMs: number) => void | Promise<void>;
  sleep?: (ms: number) => Promise<unknown>;
  random?: () => number;
}

/**
 * Run `fn`, retrying retriable errors with exponential backoff and full jitter.
 * Before retry n the delay is random(0, min(maxDelayMs, initialDelayMs * 2^(n-1))).
 * The jitter stops a fleet of consumers from hitting a recovering database in lockstep.
 */
export async function withRetry<T>(fn: () => Promise<T>, options: RetryOptions & RetryHooks): Promise<T> {
  const { retries, initialDelayMs, maxDelayMs, isRetriable, onRetry, sleep: wait = sleep, random = Math.random } = options;

  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt > retries || !isRetriable(err)) throw err;
      const delayMs = random() * Math.min(maxDelayMs, initialDelayMs * 2 ** (attempt - 1));
      await onRetry?.(err, attempt, delayMs);
      await wait(delayMs);
    }
  }
}
