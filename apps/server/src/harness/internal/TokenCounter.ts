import { type TokenCount } from "@compass/contracts";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { Prompt, Tokenizer } from "effect/unstable/ai";
import { tokenCount } from "./Ids.ts";

export class TokenCounter extends Context.Service<
  TokenCounter,
  {
    readonly count: (prompt: Prompt.Prompt) => Effect.Effect<TokenCount>;
  }
>()("@compass/server/harness/internal/TokenCounter") {
  static readonly layer = Layer.effect(
    TokenCounter,
    Effect.gen(function* () {
      const tokenizer = yield* Effect.serviceOption(Tokenizer.Tokenizer);
      const estimate = (prompt: Prompt.Prompt) => {
        const encoded = Schema.encodeSync(Prompt.Prompt)(prompt);
        return tokenCount(Math.max(1, Math.ceil(JSON.stringify(encoded).length / 4)));
      };
      return TokenCounter.of({
        count: (prompt) =>
          Option.match(tokenizer, {
            onNone: () => Effect.succeed(estimate(prompt)),
            onSome: (service) =>
              service.tokenize(prompt).pipe(
                Effect.map((tokens) => tokenCount(tokens.length)),
                Effect.orElseSucceed(() => estimate(prompt)),
              ),
          }),
      });
    }),
  );
}
