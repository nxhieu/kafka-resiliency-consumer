/**
 * Counts unknown errors with no successful message in between. One message that hits a
 * bug is parked and the consumer moves on, but a run of them means the code is broken for
 * every message, and parking them all would empty the topic into the DLQ.
 *
 * The count lives in memory, so it starts from zero again after a restart.
 */
export class Breaker {
  private failures = 0;

  constructor(readonly limit: number) {}

  /** Count one more failure. Returns true when this one trips the breaker. */
  recordFailure(): boolean {
    this.failures += 1;
    return this.failures >= this.limit;
  }

  recordSuccess(): void {
    this.failures = 0;
  }

  get failuresInARow(): number {
    return this.failures;
  }
}
