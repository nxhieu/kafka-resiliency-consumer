import type { EachBatchPayload } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import { createBatchHandler, type ProcessOne } from "../src/batch-handler.js";
import { StopConsumerError } from "../src/errors.js";

function fakeBatch(offsets: string[], stale: () => boolean = () => false) {
  const resolveOffset = vi.fn<(offset: string) => void>();
  const payload = {
    batch: { topic: "prices", partition: 0, messages: offsets.map((offset) => ({ offset, key: Buffer.from("kettle"), value: Buffer.from("{}") })) },
    resolveOffset,
    heartbeat: vi.fn(() => Promise.resolve()),
    isRunning: () => true,
    isStale: stale,
  } as unknown as EachBatchPayload;
  return { payload, resolveOffset };
}

describe("createBatchHandler", () => {
  it("passes where the message came from, and its key, to the processor, for the DLQ", async () => {
    const processOne = vi.fn<ProcessOne>(() => Promise.resolve("applied"));
    const { payload } = fakeBatch(["0"]);

    await createBatchHandler(processOne)(payload);
    expect(processOne).toHaveBeenCalledWith(
      { topic: "prices", partition: 0, offset: "0", key: Buffer.from("kettle"), value: Buffer.from("{}") },
      payload.heartbeat,
    );
  });

  it("resolves an offset only after its message has finished processing", async () => {
    let finish = () => {};
    const processOne = vi.fn<ProcessOne>(() => new Promise((resolve) => (finish = () => resolve("applied"))));
    const { payload, resolveOffset } = fakeBatch(["0"]);

    const done = createBatchHandler(processOne)(payload);
    await vi.waitFor(() => expect(processOne).toHaveBeenCalled());
    expect(resolveOffset).not.toHaveBeenCalled();

    finish();
    await done;
    expect(resolveOffset).toHaveBeenCalledWith("0");
  });

  it("on a stop, keeps the offsets before the failure and leaves the rest", async () => {
    const processOne = vi.fn<ProcessOne>(({ offset }) =>
      offset === "1" ? Promise.reject(new StopConsumerError("stop")) : Promise.resolve("applied"),
    );
    const { payload, resolveOffset } = fakeBatch(["0", "1", "2"]);

    await expect(createBatchHandler(processOne)(payload)).rejects.toBeInstanceOf(StopConsumerError);
    expect(resolveOffset.mock.calls).toEqual([["0"]]);
    expect(processOne).toHaveBeenCalledTimes(2);
  });

  it("stops early when a rebalance makes the batch stale", async () => {
    let stale = false;
    const processOne = vi.fn<ProcessOne>(() => {
      stale = true;
      return Promise.resolve("applied");
    });
    const { payload, resolveOffset } = fakeBatch(["0", "1"], () => stale);

    await createBatchHandler(processOne)(payload);
    expect(processOne).toHaveBeenCalledTimes(1);
    expect(resolveOffset.mock.calls).toEqual([["0"]]);
  });
});
