import { describe, expect, it, vi } from "vitest";
import { StopConsumerError } from "../src/errors.js";
import { processMessage } from "../src/processor.js";
import { InMemoryPriceStore, StoreTimeoutError } from "../src/store.js";
import { inbound, makeDeps } from "./helpers.js";

const heartbeat = () => Promise.resolve();
const kettle = (price: number, updatedAt: number) => ({ productId: "kettle", price, updatedAt });
const bug = { productId: "BUG", price: 1, updatedAt: 1 };

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

  it("parks a malformed message in the DLQ without retrying it", async () => {
    const store = new InMemoryPriceStore();
    const get = vi.spyOn(store, "get");
    const deps = makeDeps(store);
    const msg = inbound("{not json");

    await expect(processMessage(msg, deps, heartbeat)).resolves.toBe("dead-lettered");
    expect(get).not.toHaveBeenCalled();
    expect(deps.parked).toEqual([{ msg, kind: "poison", error: "message body is not valid JSON" }]);
  });

  it.each([
    ["no productId", { price: 1, updatedAt: 1 }],
    ["a negative price", { productId: "kettle", price: -1, updatedAt: 1 }],
    ["a price that isn't a number", { productId: "kettle", price: "1", updatedAt: 1 }],
    ["no updatedAt", { productId: "kettle", price: 1 }],
  ])("parks a message with %s", async (_, body) => {
    const deps = makeDeps(new InMemoryPriceStore());

    await expect(processMessage(inbound(body), deps, heartbeat)).resolves.toBe("dead-lettered");
    expect(deps.parked.map(({ kind }) => kind)).toEqual(["poison"]);
  });

  it("parks a message after the database fails every retry", async () => {
    const store = new InMemoryPriceStore({ failureRate: 1 });
    const get = vi.spyOn(store, "get");
    const deps = makeDeps(store);

    await expect(processMessage(inbound(kettle(25, 1)), deps, heartbeat)).resolves.toBe("dead-lettered");
    expect(get).toHaveBeenCalledTimes(6);
    expect(deps.parked.map(({ kind }) => kind)).toEqual(["transient"]);
  });

  it("parks a message that hits a bug, after retrying it", async () => {
    const store = new InMemoryPriceStore({ bugProductId: "kettle" });
    const get = vi.spyOn(store, "get");
    const deps = makeDeps(store);

    await expect(processMessage(inbound(kettle(25, 1)), deps, heartbeat)).resolves.toBe("dead-lettered");
    // Retried first, in case it's a transient error nobody has classified yet.
    expect(get).toHaveBeenCalledTimes(6);
    expect(deps.parked).toEqual([expect.objectContaining({ kind: "unknown", error: expect.stringContaining("simulated bug") })]);
  });

  it("stops on the fifth unknown error in a row, in a way KafkaJS won't retry", async () => {
    const deps = makeDeps(new InMemoryPriceStore({ bugRate: 1 }));
    for (const offset of ["1", "2", "3", "4"]) {
      await expect(processMessage(inbound(kettle(25, 1), offset), deps, heartbeat)).resolves.toBe("dead-lettered");
    }

    const result = processMessage(inbound(kettle(25, 1), "5"), deps, heartbeat);

    await expect(result).rejects.toBeInstanceOf(StopConsumerError);
    await expect(result).rejects.toMatchObject({ retriable: false });
    // The fifth isn't parked. Its offset isn't committed, so the consumer stays on it.
    expect(deps.parked.map(({ msg }) => msg.offset)).toEqual(["1", "2", "3", "4"]);
  });

  it("starts counting again after a price is saved", async () => {
    const deps = makeDeps(new InMemoryPriceStore({ bugProductId: "BUG" }));

    for (const body of [bug, bug, bug, bug, kettle(25, 1), bug, bug, bug, bug]) {
      await processMessage(inbound(body), deps, heartbeat);
    }
    expect(deps.parked).toHaveLength(8);
  });

  it("doesn't count a bad message as a success", async () => {
    const deps = makeDeps(new InMemoryPriceStore({ bugProductId: "BUG" }));
    for (const body of [bug, bug, "{not json", bug, bug]) {
      await processMessage(inbound(body), deps, heartbeat);
    }

    await expect(processMessage(inbound(bug), deps, heartbeat)).rejects.toBeInstanceOf(StopConsumerError);
  });

  it("stops when the DLQ can't be written, so the message is read again after the restart", async () => {
    const deps = makeDeps(new InMemoryPriceStore(), { deadLetter: () => Promise.reject(new Error("broker unreachable")) });

    const result = processMessage(inbound("{not json"), deps, heartbeat);

    await expect(result).rejects.toBeInstanceOf(StopConsumerError);
    await expect(result).rejects.toMatchObject({ retriable: false, cause: new Error("broker unreachable") });
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
