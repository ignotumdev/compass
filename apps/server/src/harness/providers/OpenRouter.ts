import { type HarnessModelBinding, type ModelKey, ProviderKey } from "@compass/contracts";
import { OpenRouterClient, OpenRouterLanguageModel } from "@effect/ai-openrouter";
import { Config, Effect, Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { CompactionModel, ConversationModel } from "../Models.ts";

const provider = ProviderKey.make("openrouter");

const binding = (
  model: ModelKey,
  client: Layer.Layer<OpenRouterClient.OpenRouterClient>,
): HarnessModelBinding => ({
  provider,
  model,
  layer: OpenRouterLanguageModel.layer({ model }).pipe(Layer.provide(client)),
});

export const layers = (options: {
  readonly conversationModel: ModelKey;
  readonly compactionModel?: ModelKey;
  readonly apiKey?: Redacted.Redacted<string>;
}) => {
  const client = OpenRouterClient.layer({ apiKey: options.apiKey }).pipe(
    Layer.provide(FetchHttpClient.layer),
  );
  const conversation = binding(options.conversationModel, client);
  const compaction = binding(options.compactionModel ?? options.conversationModel, client);
  return Layer.merge(ConversationModel.layer(conversation), CompactionModel.layer(compaction));
};

export const layersConfig = (options: {
  readonly conversationModel: ModelKey;
  readonly compactionModel?: ModelKey;
  readonly apiKey?: Config.Config<Redacted.Redacted<string>>;
}) =>
  Layer.unwrap(
    Effect.map(options.apiKey ?? Config.redacted("OPENROUTER_API_KEY"), (apiKey) =>
      layers({ ...options, apiKey }),
    ),
  );
