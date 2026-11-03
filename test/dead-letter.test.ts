import type { Producer } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import { DLQ_HEADERS, kafkaDeadLetters, toDeadLetterMessage } from "../src/dead-letter.js";
import { inbound } from "./helpers.js";

describe("toDeadLetterMessage", () => {
  it("keeps the original key and value, and records where the message came from and why it failed", () => {
    const msg = { ...inbound("{not json"), partition: 2, offset: "7" };

    const message = toDeadLetterMessage({ msg, kind: "poison", error: "message body is not valid JSON" }, new Date("2026-11-03T03:00:00.000Z"));

    expect(message.key).toBe(msg.key);
    expect(message.value).toBe(msg.value);
    expect(message.headers).toEqual({
      [DLQ_HEADERS.topic]: "prices",
      [DLQ_HEADERS.partition]: "2",
      [DLQ_HEADERS.offset]: "7",
      [DLQ_HEADERS.kind]: "poison",
      [DLQ_HEADERS.error]: "message body is not valid JSON",
      [DLQ_HEADERS.failedAt]: "2026-11-03T03:00:00.000Z",
    });
  });
});

describe("kafkaDeadLetters", () => {
  it("sends the message to the DLQ topic", async () => {
    const send = vi.fn(() => Promise.resolve([]));
    const msg = inbound("{not json");

    await kafkaDeadLetters({ send } as unknown as Producer, "prices.dlq")({ msg, kind: "poison", error: "bad" });

    expect(send).toHaveBeenCalledWith({ topic: "prices.dlq", messages: [expect.objectContaining({ key: msg.key, value: msg.value })] });
  });
});
