import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import { Prompt, Response } from "effect/unstable/ai";
import {
  EventBufferSize,
  MessageId,
  PromptInput,
  ResponsePartEvent,
  SessionEvent,
  SessionId,
  TokenLimit,
  TurnId,
} from "./harness.ts";

describe("harness contracts", () => {
  it.effect("decodes branded identifiers and bounded settings", () =>
    Effect.gen(function* () {
      const sessionId = yield* Schema.decodeUnknownEffect(SessionId)(
        "0198ee50-2c74-7000-8000-000000000001",
      );
      const compactAt = yield* Schema.decodeUnknownEffect(TokenLimit)(8_000);
      const buffer = yield* Schema.decodeUnknownEffect(EventBufferSize)(64);

      expect(sessionId).toBe("0198ee50-2c74-7000-8000-000000000001");
      expect(compactAt).toBe(8_000);
      expect(buffer).toBe(64);
    }),
  );

  it.effect("rejects malformed branded values", () =>
    Effect.gen(function* () {
      expect(
        Option.isNone(yield* Effect.option(Schema.decodeUnknownEffect(SessionId)("nope"))),
      ).toBe(true);
      expect(Option.isNone(yield* Effect.option(Schema.decodeUnknownEffect(TokenLimit)(0)))).toBe(
        true,
      );
    }),
  );

  it("keeps stream input variants schema-backed", () => {
    const input = new PromptInput({
      message: Prompt.userMessage({
        content: [Prompt.textPart({ text: "hello" })],
      }),
    });

    expect(input._tag).toBe("Prompt");
    expect(input.message.role).toBe("user");
  });

  it.effect("validates streamed Effect AI response parts", () =>
    Effect.gen(function* () {
      const base = {
        sessionId: SessionId.make("0198ee50-2c74-7000-8000-000000000001"),
        turnId: TurnId.make("0198ee50-2c74-7000-8000-000000000002"),
        messageId: MessageId.make("0198ee50-2c74-7000-8000-000000000003"),
      };
      const valid = new ResponsePartEvent({
        ...base,
        part: Response.makePart("text-delta", { id: "text", delta: "hello" }),
      });

      expect(Option.isSome(yield* Effect.option(Schema.encodeEffect(SessionEvent)(valid)))).toBe(
        true,
      );
      expect(
        Option.isNone(
          yield* Effect.option(
            Schema.decodeUnknownEffect(SessionEvent)({
              _tag: "ResponsePart",
              ...base,
              part: { type: "text-delta", id: "text", delta: 42 },
            }),
          ),
        ),
      ).toBe(true);
    }),
  );
});
