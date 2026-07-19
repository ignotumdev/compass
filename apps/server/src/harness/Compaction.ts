import {
  type MessageId,
  MessageId as MessageIdSchema,
  SessionCompactionError,
  type StoredMessage,
  StoredMessage as StoredMessageSchema,
  type StoredSession,
  type TokenCount,
  type TurnId,
} from "@compass/contracts";
import { Context, Crypto, Effect, Layer, Schema } from "effect";
import { LanguageModel, Prompt } from "effect/unstable/ai";
import { CompactionModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { now } from "./internal/Ids.ts";

const SUMMARY_SYSTEM_INSTRUCTIONS = `You compact agent conversation history.
Produce a concise but complete working summary of the supplied history.
Preserve user intent, decisions, constraints, tool activity, concrete results, errors, and unfinished work.
Do not address the user and do not invent facts.`;

const serializeMessages = (messages: ReadonlyArray<StoredMessage>) =>
  JSON.stringify(messages.map((stored) => Schema.encodeSync(Prompt.Message)(stored.message)));

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

          const response = yield* languageModel.generateText({
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

          const summaryText = response.text.trim();
          if (summaryText.length === 0) {
            return yield* new SessionCompactionError({
              sessionId: session.id,
              message: "The compaction model returned an empty summary",
              cause: response.finishReason,
            });
          }

          const [id, sequence, timestamp] = yield* Effect.all([
            Effect.map(crypto.randomUUIDv7, (value) => MessageIdSchema.make(value)),
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
