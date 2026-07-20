import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import {
  ChannelId,
  GatewayFile,
  GatewayTimestampMillis,
  IncomingMessage,
  GatewayMessageId,
  MessageSender,
  parseChannelId,
} from "./gateway.ts";

describe("gateway contracts", () => {
  it.effect("decodes adapter-qualified channels and gateway timestamps", () =>
    Effect.gen(function* () {
      const channel = yield* Schema.decodeUnknownEffect(ChannelId)("telegram:123");
      const receivedAt = yield* Schema.decodeUnknownEffect(GatewayTimestampMillis)(1_000);

      expect(parseChannelId(channel)).toEqual({ adapter: "telegram", nativeChannel: "123" });
      expect(receivedAt).toBe(1_000);
    }),
  );

  it.effect("rejects malformed gateway identifiers", () =>
    Effect.gen(function* () {
      expect(
        Option.isNone(yield* Effect.option(Schema.decodeUnknownEffect(ChannelId)("123"))),
      ).toBe(true);
      expect(
        Option.isNone(yield* Effect.option(Schema.decodeUnknownEffect(GatewayTimestampMillis)(-1))),
      ).toBe(true);
    }),
  );

  it("keeps incoming messages and files schema-backed", () => {
    const message = new IncomingMessage({
      id: GatewayMessageId.make("1"),
      channel: ChannelId.make("telegram:123"),
      sender: new MessageSender({ id: "7", displayName: "Ada", isBot: false }),
      files: [new GatewayFile({ path: "C:/files/photo.jpg", name: "photo.jpg", kind: "image" })],
      receivedAt: GatewayTimestampMillis.make(1_000),
    });

    expect(message.files[0]?.kind).toBe("image");
  });
});
