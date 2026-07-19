import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";
import { TokenCounter } from "./TokenCounter.ts";

describe("TokenCounter", () => {
  it.effect("estimates fallback tokens from UTF-8 bytes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(TokenCounter.layer);
        const counter = Context.get(context, TokenCounter);
        const prompt = Prompt.fromMessages([
          Prompt.userMessage({
            content: [Prompt.textPart({ text: "\u6f22\u5b57\u{1f642}" })],
          }),
        ]);
        const encoded = yield* Schema.encodeEffect(Prompt.Prompt)(prompt);
        const expected = Math.max(
          1,
          Math.ceil(new TextEncoder().encode(JSON.stringify(encoded)).byteLength / 4),
        );

        expect(yield* counter.count(prompt)).toBe(expected);
        expect(expected).toBeGreaterThan(Math.ceil(JSON.stringify(encoded).length / 4));
      }),
    ),
  );
});
