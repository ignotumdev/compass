import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  EventBufferSize,
  ModelKey,
  PromptInput,
  ProviderKey,
  QueueInput,
  SessionConfiguration,
  SessionId,
  SteerInput,
  StoredSession,
  TimestampMillis,
  TokenLimit,
  type SessionEvent,
} from "@compass/contracts";
import { Context, Effect, Latch, Layer, Ref, Stream } from "effect";
import { LanguageModel, Prompt, type Response } from "effect/unstable/ai";
import { AgentSession } from "./AgentSession.ts";
import { Compactor } from "./Compaction.ts";
import { Instructions } from "./Instructions.ts";
import { ConversationModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { TokenCounter } from "./internal/TokenCounter.ts";

const finishPart: Response.FinishPartEncoded = {
  type: "finish",
  reason: "stop",
  usage: {
    inputTokens: {
      uncached: 1,
      total: 1,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
};

const textResponse = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "text" },
  { type: "text-delta", id: "text", delta: text },
  { type: "text-end", id: "text" },
  finishPart,
];

const userMessage = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] });

const promptText = (message: Prompt.Message): string =>
  typeof message.content === "string"
    ? message.content
    : message.content
        .filter((part): part is Prompt.TextPart => part.type === "text")
        .map((part) => part.text)
        .join("");

describe("AgentSession", () => {
  it.effect("streams events and applies steer before queued turns", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const firstGenerationStarted = yield* Latch.make();
        const releaseFirstGeneration = yield* Latch.make();
        const capturedPrompts: Array<Prompt.Prompt> = [];
        let calls = 0;
        const languageModelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (options) => {
              capturedPrompts.push(options.prompt);
              calls += 1;
              if (calls === 1) {
                const parts: ReadonlyArray<Response.StreamPartEncoded> = [
                  {
                    type: "tool-call",
                    id: "execute-1",
                    name: "execute",
                    params: { command: "pwd" },
                  },
                  finishPart,
                ];
                return Stream.concat(
                  Stream.fromEffect(firstGenerationStarted.open).pipe(Stream.drain),
                  Stream.concat(
                    Stream.fromEffect(releaseFirstGeneration.await).pipe(Stream.drain),
                    Stream.fromIterable(parts),
                  ),
                );
              }
              return Stream.fromIterable(textResponse(`answer-${calls}`));
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("scripted"),
          layer: languageModelLayer,
        };
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000011"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "Keep going after steering.",
            compactAtTokens: TokenLimit.make(100_000),
            summaryMaxTokens: TokenLimit.make(1_000),
            eventBufferSize: EventBufferSize.make(128),
          }),
          createdAt: TimestampMillis.make(1_700_000_000_000),
          updatedAt: TimestampMillis.make(1_700_000_000_000),
        });
        const dependencies = Layer.mergeAll(
          SessionStore.layerMemory,
          Instructions.layer,
          TokenCounter.layer,
          ConversationModel.layer(binding),
          Layer.succeed(
            Compactor,
            Compactor.of({
              compact: () => Effect.die("compaction was not expected"),
            }),
          ),
          NodeCrypto.layer,
        );
        const context = yield* Layer.build(
          AgentSession.layer(session).pipe(Layer.provideMerge(dependencies)),
        );
        const agent = Context.get(context, AgentSession);
        const store = Context.get(context, SessionStore);
        yield* store.createSession(session);

        const events = yield* Ref.make<ReadonlyArray<SessionEvent>>([]);
        yield* agent.events.pipe(
          Stream.runForEach((event) => Ref.update(events, (current) => [...current, event])),
          Effect.forkScoped,
        );

        yield* agent.offer(new PromptInput({ message: userMessage("initial") }));
        yield* firstGenerationStarted.await;
        yield* agent.offer(new SteerInput({ message: userMessage("steer-now") }));
        yield* agent.offer(new QueueInput({ message: userMessage("later") }));
        yield* releaseFirstGeneration.open;
        yield* agent.waitForIdle;

        const active = yield* store.activeMessages(session.id);
        const userTexts = active
          .filter((message) => message.message.role === "user")
          .map((message) => promptText(message.message));
        const emitted = yield* Ref.get(events);

        expect(
          emitted.filter((event) => event._tag === "SessionFailed").map((event) => event.message),
        ).toEqual([]);
        expect(calls).toBe(3);
        expect(userTexts).toEqual(["initial", "steer-now", "later"]);
        expect(
          capturedPrompts[1]?.content.some((message) => promptText(message) === "steer-now"),
        ).toBe(true);
        expect(capturedPrompts[1]?.content.some((message) => message.role === "tool")).toBe(true);
        expect(emitted.some((event) => event._tag === "ResponsePart")).toBe(true);
        expect(emitted.filter((event) => event._tag === "TurnCompleted")).toHaveLength(2);
      }),
    ),
  );
});
