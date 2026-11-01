import { describe, expect, it, vi } from "vitest";
import { withRetry } from "../src/retry.js";

const options = {
  retries: 5,
  initialDelayMs: 100,
  maxDelayMs: 1000,
  isRetriable: () => true,
  sleep: () => Promise.resolve(),
};

describe("withRetry", () => {
  it("returns as soon as the call succeeds", async () => {
    const fn = vi.fn<() => Promise<string>>().mockRejectedValueOnce(new Error("blip")).mockResolvedValue("ok");

    await expect(withRetry(fn, options)).resolves.toBe("ok");
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("gives up after the configured number of retries", async () => {
    const error = new Error("down");
    const fn = vi.fn(() => Promise.reject(error));

    await expect(withRetry(fn, options)).rejects.toBe(error);
    expect(fn).toHaveBeenCalledTimes(6);
  });

  it("doesn't retry an error that isn't retriable", async () => {
    const fn = vi.fn(() => Promise.reject(new Error("bad input")));

    await expect(withRetry(fn, { ...options, isRetriable: () => false })).rejects.toThrow("bad input");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("backs off exponentially up to the cap, scaled by the jitter", async () => {
    const delays: number[] = [];
    const fn = () => Promise.reject(new Error("down"));

    await withRetry(fn, { ...options, random: () => 1, sleep: (ms) => Promise.resolve(delays.push(ms)) }).catch(() => {});
    expect(delays).toEqual([100, 200, 400, 800, 1000]);

    delays.length = 0;
    await withRetry(fn, { ...options, random: () => 0.5, sleep: (ms) => Promise.resolve(delays.push(ms)) }).catch(() => {});
    expect(delays).toEqual([50, 100, 200, 400, 500]);
  });
});
