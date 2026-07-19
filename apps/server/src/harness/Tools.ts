import { Effect, Ref, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

export const ExecuteTool = Tool.make("execute", {
  description: "Execute a command in the Compass environment.",
  parameters: Schema.Struct({
    command: Schema.String.annotate({
      description: "The command to execute.",
    }),
  }),
  success: Schema.String,
});

export const CompactTool = Tool.make("compact", {
  description:
    "Compact the conversation history when the context is too large or a fresh summary would help.",
  parameters: Schema.Struct({}),
  success: Schema.String,
});

export const HarnessToolkit = Toolkit.make(ExecuteTool, CompactTool);

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
