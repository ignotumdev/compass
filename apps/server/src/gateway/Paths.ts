import { Config, Effect, Path } from "effect";

export const compassDirectory = Effect.gen(function* () {
  const path = yield* Path.Path;
  const home = yield* Config.string("HOME").pipe(Config.orElse(() => Config.string("USERPROFILE")));
  return path.join(home, ".compass");
});
