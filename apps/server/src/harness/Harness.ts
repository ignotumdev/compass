import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Layer } from "effect";
import { Tokenizer } from "effect/unstable/ai";
import { AgentHarness } from "./AgentHarness.ts";
import { Compactor } from "./Compaction.ts";
import { Instructions } from "./Instructions.ts";
import { type CompactionModel, type ConversationModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { TokenCounter } from "./internal/TokenCounter.ts";
import * as SqliteSessionStore from "./persistence/SqliteSessionStore.ts";

interface HarnessLayerOptions {
  readonly store?: Layer.Layer<SessionStore>;
  readonly instructions?: Layer.Layer<Instructions>;
  readonly tokenCounter?: Layer.Layer<TokenCounter>;
  readonly tokenizer?: Layer.Layer<Tokenizer.Tokenizer>;
}

/**
 * Assembles the provider-neutral harness. Model adapters only need to provide
 * the conversation and compaction model bindings.
 */
export const layer = <E, R>(
  models: Layer.Layer<ConversationModel | CompactionModel, E, R>,
  options: HarnessLayerOptions = {},
) => {
  const store =
    options.store ?? SqliteSessionStore.layerDefault.pipe(Layer.provide(NodeServices.layer));
  const foundations = Layer.mergeAll(
    models,
    store,
    options.instructions ?? Instructions.layer,
    NodeCrypto.layer,
    ...(options.tokenizer === undefined ? [] : [options.tokenizer]),
  );
  const withTokenCounter = (options.tokenCounter ?? TokenCounter.layer).pipe(
    Layer.provideMerge(foundations),
  );
  const services = Compactor.layer.pipe(Layer.provideMerge(withTokenCounter));
  return AgentHarness.layer.pipe(Layer.provide(services));
};
