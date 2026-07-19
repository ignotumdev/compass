import { type HarnessModelBinding } from "@compass/contracts";
import { Context, Effect, Layer } from "effect";

export class ConversationModel extends Context.Service<ConversationModel, HarnessModelBinding>()(
  "@compass/server/harness/Models/ConversationModel",
) {
  static readonly layer = (binding: HarnessModelBinding) =>
    Layer.succeed(ConversationModel, ConversationModel.of(binding));
}

export class CompactionModel extends Context.Service<CompactionModel, HarnessModelBinding>()(
  "@compass/server/harness/Models/CompactionModel",
) {
  static readonly layer = (binding: HarnessModelBinding) =>
    Layer.succeed(CompactionModel, CompactionModel.of(binding));
}

export const compactionModelFromConversation = Layer.effect(
  CompactionModel,
  Effect.map(ConversationModel, (binding) => CompactionModel.of(binding)),
);
