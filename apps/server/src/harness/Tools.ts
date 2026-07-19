import { CompactTool, ExecuteTool, HarnessToolkit } from "@compass/contracts";
import { Effect, Ref } from "effect";

export { CompactTool, ExecuteTool, HarnessToolkit };

const handlers = (compactionRequested: Ref.Ref<boolean>) =>
  HarnessToolkit.of({
    execute: Effect.fn("HarnessTool.execute")(() => Effect.succeed("Not implemented yet.")),
    compact: Effect.fn("HarnessTool.compact")(function* () {
      yield* Ref.set(compactionRequested, true);
      return "Compaction scheduled.";
    }),
  });

export const makeToolkit = (compactionRequested: Ref.Ref<boolean>) =>
  Effect.gen(function* () {
    const context = yield* HarnessToolkit.toHandlers(handlers(compactionRequested));
    return yield* HarnessToolkit.pipe(Effect.provideContext(context));
  });

export const toolkitLayer = (compactionRequested: Ref.Ref<boolean>) =>
  HarnessToolkit.toLayer(Effect.succeed(handlers(compactionRequested)));
