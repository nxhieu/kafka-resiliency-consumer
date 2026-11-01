import { describe, expect, it, vi } from "vitest";
import { StopConsumerError } from "../src/errors.js";
import { processMessage } from "../src/processor.js";
import { InMemoryPriceStore, StoreTimeoutError } from "../src/store.js";
import { inbound, makeDeps } from "./helpers.js";

const heartbeat = () => Promise.resolve();
const kettle = (price: number, updatedAt: number) => ({ productId: "kettle", price, updatedAt });

describe("processMessage", () => {
  it("saves a valid price", async () => {
    const store = new InMemoryPriceStore();

    await expect(processMessage(inbound(kettle(25, 1)), makeDeps(store), heartbeat)).resolves.toBe("applied");
    expect(store.peek("kettle")?.value).toEqual(kettle(25, 1));
  });

  it("ignores a price older than the one it has, so replays are harmless", async () => {
    const store = new InMemoryPriceStore();
    await store.put(kettle(25, 2), 0);

    await expect(processMessage(inbound(kettle(30, 1)), makeDeps(store), heartbeat)).resolves.toBe("stale");
    expect(store.peek("kettle")?.value).toEqual(kettle(25, 2));
  });

  it("retries transient database errors until they succeed", async () => {
    const store = new InMemoryPriceStore();
    const get = vi
      .spyOn(store, "get")
      .mockRejectedValueOnce(new StoreTimeoutError("get timed out"))
      .mockRejectedValueOnce(new StoreTimeoutError("get timed out"));
    const beat = vi.fn(heartbeat);

    await expect(processMessage(inbound(kettle(25, 1)), makeDeps(store), beat)).resolves.toBe("applied");
    expect(get).toHaveBeenCalledTimes(3);
    expect(beat).toHaveBeenCalledTimes(2);
  });

  it("skips a malformed message without retrying it", async () => {
    const store = new InMemoryPriceStore();
    const get = vi.spyOn(store, "get");

    await expect(processMessage(inbound("{not json"), makeDeps(store), heartbeat)).resolves.toBe("skipped");
    expect(get).not.toHaveBeenCalled();
  });

  it.each([
    ["no productId", { price: 1, updatedAt: 1 }],
    ["a negative price", { productId: "kettle", price: -1, updatedAt: 1 }],
    ["a price that isn't a number", { productId: "kettle", price: "1", updatedAt: 1 }],
    ["no updatedAt", { productId: "kettle", price: 1 }],
  ])("skips a message with %s", async (_, body) => {
    await expect(processMessage(inbound(body), makeDeps(new InMemoryPriceStore()), heartbeat)).resolves.toBe("skipped");
  });

  it("skips after the database fails every retry", async () => {
    const store = new InMemoryPriceStore({ failureRate: 1 });
    const get = vi.spyOn(store, "get");

    await expect(processMessage(inbound(kettle(25, 1)), makeDeps(store), heartbeat)).resolves.toBe("skipped");
    expect(get).toHaveBeenCalledTimes(6);
  });

  it("stops on an unknown error, in a way KafkaJS won't retry", async () => {
    const store = new InMemoryPriceStore({ bugProductId: "kettle" });
    const get = vi.spyOn(store, "get");

    const result = processMessage(inbound(kettle(25, 1)), makeDeps(store), heartbeat);

    await expect(result).rejects.toBeInstanceOf(StopConsumerError);
    await expect(result).rejects.toMatchObject({ retriable: false });
    // Retried first, in case it's a transient error nobody has classified yet.
    expect(get).toHaveBeenCalledTimes(6);
  });

  it("re-reads and retries when another writer gets in between the read and the write", async () => {
    const store = new InMemoryPriceStore();
    await store.put(kettle(25, 1), 0);
    const realGet = store.get.bind(store);
    vi.spyOn(store, "get").mockImplementationOnce(async (productId) => {
      const snapshot = await realGet(productId);
      // Another consumer saves a newer price after our read, before our write.
      await store.put(kettle(27, 3), 1);
      return snapshot;
    });

    // Our update (time 2) is older than theirs (time 3). Without the version check it
    // would overwrite the newer price.
    await expect(processMessage(inbound(kettle(26, 2)), makeDeps(store), heartbeat)).resolves.toBe("stale");
    expect(store.peek("kettle")?.value).toEqual(kettle(27, 3));
  });
});
