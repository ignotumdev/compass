import {
  type MessageId,
  ModelGenerationOptions,
  SessionCompactionError,
  type StoredMessage,
  StoredMessage as StoredMessageSchema,
  type StoredSession,
  type TokenCount,
  type TurnId,
} from "@compass/contracts";
import { Context, Crypto, Effect, Layer, Option, Schema } from "effect";
import { LanguageModel, Prompt, Tokenizer } from "effect/unstable/ai";
import { CompactionModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { makeMessageId, now } from "./internal/Ids.ts";

const SUMMARY_SYSTEM_INSTRUCTIONS = `You compact agent conversation history.
Produce a concise but complete working summary of the supplied history.
Preserve user intent, decisions, constraints, tool activity, concrete results, errors, and unfinished work.
Do not address the user and do not invent facts.`;

const serializeMessages = (messages: ReadonlyArray<StoredMessage>) =>
  JSON.stringify(messages.map((stored) => Schema.encodeSync(Prompt.Message)(stored.message)));

const limitSummaryTextByUtf8Bytes = (text: string, maximumBytes: number): string => {
  const encoder = new TextEncoder();
  if (encoder.encode(text).byteLength <= maximumBytes) return text;

  const characters: Array<string> = [];
  let bytes = 0;
  for (const character of text) {
    const characterBytes = encoder.encode(character).byteLength;
    if (bytes + characterBytes > maximumBytes) break;
    characters.push(character);
    bytes += characterBytes;
  }
  return characters.join("").trimEnd();
};

const limitSummaryTextWithTokenizer = Effect.fn("Compactor.limitSummaryTextWithTokenizer")(
  function* (text: string, maxTokens: number, tokenizer: Tokenizer.Service) {
    const tokens = yield* tokenizer.tokenize(text);
    if (tokens.length <= maxTokens) return text;

    const characters = Array.from(text);
    let characterCount = Math.max(
      1,
      Math.min(characters.length - 1, Math.floor((characters.length * maxTokens) / tokens.length)),
    );

    while (characterCount > 0) {
      const candidate = characters.slice(0, characterCount).join("").trimEnd();
      if (candidate.length === 0) return "";

      const candidateTokens = yield* tokenizer.tokenize(candidate);
      if (candidateTokens.length <= maxTokens) return candidate;

      characterCount = Math.min(
        characterCount - 1,
        Math.floor((characterCount * maxTokens) / candidateTokens.length),
      );
    }

    return "";
  },
);

/**
 * Enforces the model-specific token limit when an Effect AI Tokenizer is
 * installed. Without one, each permitted token is conservatively treated as
 * one UTF-8 byte instead of relying on a bytes-per-token average.
 */
export const limitSummaryText = Effect.fn("Compactor.limitSummaryText")(function* (
  text: string,
  maxTokens: number,
) {
  const tokenizer = yield* Effect.serviceOption(Tokenizer.Tokenizer);
  return yield* Option.match(tokenizer, {
    onNone: () => Effect.succeed(limitSummaryTextByUtf8Bytes(text, maxTokens)),
    onSome: (service) => limitSummaryTextWithTokenizer(text, maxTokens, service),
  });
});

export class Compactor extends Context.Service<
  Compactor,
  {
    readonly compact: (
      session: StoredSession,
      activeMessages: ReadonlyArray<StoredMessage>,
      latestUserMessageId: MessageId,
      turnId: TurnId,
      tokensBefore: TokenCount,
    ) => Effect.Effect<StoredMessage, SessionCompactionError>;
  }
>()("@compass/server/harness/Compaction/Compactor") {
  static readonly layer = Layer.effect(
    Compactor,
    Effect.gen(function* () {
      const store = yield* SessionStore;
      const crypto = yield* Crypto.Crypto;
      const binding = yield* CompactionModel;
      const modelContext = yield* Layer.build(binding.layer);
      const languageModel = Context.get(modelContext, LanguageModel.LanguageModel);

      const compact = Effect.fn("Compactor.compact")(
        function* (
          session: StoredSession,
          activeMessages: ReadonlyArray<StoredMessage>,
          latestUserMessageId: MessageId,
          turnId: TurnId,
          _tokensBefore: TokenCount,
        ) {
          const latestIndex = activeMessages.findIndex(
            (message) => message.id === latestUserMessageId,
          );
          if (latestIndex < 0) {
            return yield* new SessionCompactionError({
              sessionId: session.id,
              message: "The latest user message is not in the active context",
              cause: latestUserMessageId,
            });
          }
          const prefix = activeMessages.slice(0, latestIndex);
          if (prefix.length === 0) {
            return yield* new SessionCompactionError({
              sessionId: session.id,
              message: "There is no conversation history to compact",
              cause: "empty-prefix",
            });
          }

          const generation = languageModel.generateText({
            prompt: Prompt.fromMessages([
              Prompt.makeMessage("system", {
                content: SUMMARY_SYSTEM_INSTRUCTIONS,
              }),
              Prompt.userMessage({
                content: [
                  Prompt.textPart({
                    text: `<conversation>\n${serializeMessages(prefix)}\n</conversation>\n\nSummarize the conversation for the agent that will continue it.`,
                  }),
                ],
              }),
            ]),
            toolChoice: "none",
          });
          const generationWithLimit =
            binding.transformGeneration === undefined
              ? generation
              : binding.transformGeneration(
                  generation,
                  new ModelGenerationOptions({
                    maxOutputTokens: session.configuration.summaryMaxTokens,
                  }),
                );
          const response = yield* generationWithLimit;

          const summaryText = yield* limitSummaryText(
            response.text.trim(),
            session.configuration.summaryMaxTokens,
          );
          if (summaryText.length === 0) {
            return yield* new SessionCompactionError({
              sessionId: session.id,
              message: "The compaction model returned an empty summary",
              cause: response.finishReason,
            });
          }

          const [id, sequence, timestamp] = yield* Effect.all([
            makeMessageId(crypto),
            store.nextMessageSequence(session.id),
            now,
          ]);
          const summary = new StoredMessageSchema({
            id,
            sessionId: session.id,
            turnId,
            sequence,
            message: Prompt.userMessage({
              content: [
                Prompt.textPart({
                  text: `Conversation summary:\n\n${summaryText}`,
                }),
              ],
            }),
            status: "complete",
            createdAt: timestamp,
            updatedAt: timestamp,
          });
          yield* store.commitCompaction(summary, latestUserMessageId);
          return summary;
        },
        (effect, session) =>
          Effect.mapError(effect, (cause) =>
            Schema.is(SessionCompactionError)(cause)
              ? cause
              : new SessionCompactionError({
                  sessionId: session.id,
                  message: "Conversation compaction failed",
                  cause,
                }),
          ),
      );

      return Compactor.of({ compact });
    }),
  );
}
