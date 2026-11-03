import { TransientError } from "./errors.js";

export interface Price {
  productId: string;
  price: number;
  /** When the price changed at the source, in epoch milliseconds. */
  updatedAt: number;
}

export interface Versioned<T> {
  value: T;
  version: number;
}

/** The `prices` collection. */
export interface PriceStore {
  get(productId: string): Promise<Versioned<Price> | undefined>;
  /**
   * Write only if the stored version still equals `expectedVersion` (0 means "doesn't
   * exist yet"). Otherwise throw VersionConflictError.
   */
  put(price: Price, expectedVersion: number): Promise<void>;
}

export class StoreTimeoutError extends TransientError {
  name = "StoreTimeoutError";
}

/** Transient: someone wrote between our read and our write. A fresh read fixes it. */
export class VersionConflictError extends TransientError {
  name = "VersionConflictError";
}

export interface FaultOptions {
  /** Fraction of calls (0 to 1) that time out, to simulate a flaky database. */
  failureRate?: number;
  /** Calls for this product throw a TypeError, to simulate a bug. */
  bugProductId?: string;
  /** Fraction of calls (0 to 1) that throw a TypeError. 1 simulates a deploy that broke everything. */
  bugRate?: number;
  random?: () => number;
}

/** Stand-in for a document database, with optimistic locking and fault injection. */
export class InMemoryPriceStore implements PriceStore {
  private readonly docs = new Map<string, Versioned<Price>>();

  constructor(private readonly faults: FaultOptions = {}) {}

  async get(productId: string): Promise<Versioned<Price> | undefined> {
    await this.simulateCall("get", productId);
    const doc = this.docs.get(productId);
    return doc && { value: { ...doc.value }, version: doc.version };
  }

  async put(price: Price, expectedVersion: number): Promise<void> {
    await this.simulateCall("put", price.productId);
    const version = this.docs.get(price.productId)?.version ?? 0;
    if (version !== expectedVersion) {
      throw new VersionConflictError(`${price.productId} is at version ${version}, expected ${expectedVersion}`);
    }
    this.docs.set(price.productId, { value: { ...price }, version: version + 1 });
  }

  /** Read without fault injection, for tests. */
  peek(productId: string): Versioned<Price> | undefined {
    return this.docs.get(productId);
  }

  private async simulateCall(operation: string, productId: string): Promise<void> {
    await Promise.resolve();
    const { failureRate = 0, bugProductId, bugRate = 0, random = Math.random } = this.faults;
    if (productId === bugProductId || random() < bugRate) throw new TypeError(`Cannot read properties of undefined (simulated bug in ${operation})`);
    if (random() < failureRate) throw new StoreTimeoutError(`${operation} timed out`);
  }
}
