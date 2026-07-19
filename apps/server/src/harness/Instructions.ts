import type { StoredSession } from "@compass/contracts";
import { Context, Effect, Layer } from "effect";
import { Prompt } from "effect/unstable/ai";

export class Instructions extends Context.Service<
  Instructions,
  {
    readonly build: (session: StoredSession) => Effect.Effect<Prompt.Prompt>;
  }
>()("@compass/server/harness/Instructions") {
  static readonly layer = Layer.succeed(
    Instructions,
    Instructions.of({
      build: (session) =>
        Effect.succeed(
          session.configuration.systemInstructions.length === 0
            ? Prompt.empty
            : Prompt.fromMessages([
                Prompt.makeMessage("system", {
                  content: session.configuration.systemInstructions,
                }),
              ]),
        ),
    }),
  );

  static readonly layerCustom = (build: (session: StoredSession) => Effect.Effect<Prompt.Prompt>) =>
    Layer.succeed(Instructions, Instructions.of({ build }));
}
