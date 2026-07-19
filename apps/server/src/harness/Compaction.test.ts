import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { Prompt, Tokenizer } from "effect/unstable/ai";
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
