import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { AiError, Prompt, Tokenizer } from "effect/unstable/ai";
import { limitSummaryText } from "./Compaction.ts";

describe("Compaction", () => {
  it.effect("uses the installed tokenizer to enforce the configured token limit", () =>
    Effect.gen(function* () {
      const tokenHeavyText = "!".repeat(20);
      const tokenizer = Tokenizer.make({
        tokenize: (prompt) =>
          Effect.succeed(
            extractText(prompt)
              .split("")
              .map((_, index) => index),
          ),
      });

      const limited = yield* limitSummaryText(tokenHeavyText, 3).pipe(
        Effect.provideService(Tokenizer.Tokenizer, tokenizer),
      );
      const tokens = yield* tokenizer.tokenize(limited);

      expect(limited).toBe("!!!");
      expect(tokens.length).toBeLessThanOrEqual(3);
    }),
  );

  it.effect("falls back to a conservative UTF-8 byte limit", () =>
    Effect.gen(function* () {
      const limited = yield* limitSummaryText("\u00e9".repeat(20), 3);

      expect(new TextEncoder().encode(limited).byteLength).toBeLessThanOrEqual(3);
      expect(limited).toBe("\u00e9");
    }),
  );

  it.effect("falls back when the installed tokenizer fails", () =>
    Effect.gen(function* () {
      const tokenizer = Tokenizer.make({
        tokenize: () =>
          Effect.fail(
            AiError.make({
              module: "CompactionTest",
              method: "tokenize",
              reason: new AiError.InternalProviderError({ description: "tokenizer failed" }),
            }),
          ),
      });

      const limited = yield* limitSummaryText("abcdefgh", 3).pipe(
        Effect.provideService(Tokenizer.Tokenizer, tokenizer),
      );

      expect(limited).toBe("abc");
    }),
  );

  it.effect("does not split a multi-byte code point in the fallback", () =>
    Effect.gen(function* () {
      expect(yield* limitSummaryText("\u{1f642}\u{1f642}", 4)).toBe("\u{1f642}");
    }),
  );
});

const extractText = (prompt: Prompt.Prompt): string =>
  prompt.content
    .flatMap((message) =>
      typeof message.content === "string"
        ? [message.content]
        : message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])),
    )
    .join("");
