import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  EventBufferSize,
  MessageId,
  MessageSequence,
  ModelKey,
  PendingInputId,
  PromptInput,
  ProviderKey,
  QueueInput,
  SessionConfiguration,
  SessionId,
  SteerInput,
  StoredMessage,
  StoredPendingInput,
  StoredSession,
  StoredTurn,
  TimestampMillis,
  TokenLimit,
  TurnId,
  type SessionEvent,
} from "@compass/contracts";
import { Context, Effect, Fiber, Latch, Layer, Option, Ref, Stream } from "effect";
import { AiError, LanguageModel, Prompt, type Response } from "effect/unstable/ai";
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
        const toolCallObserved = yield* Latch.make();
        const releaseToolResult = yield* Latch.make();
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
                return Stream.concat(
                  Stream.succeed({
                    type: "tool-call",
                    id: "execute-1",
                    name: "execute",
                    params: { command: "pwd" },
                    providerExecuted: true,
                  } satisfies Response.StreamPartEncoded),
                  Stream.concat(
                    Stream.fromEffect(releaseToolResult.await).pipe(Stream.drain),
                    Stream.succeed({
                      type: "tool-result",
                      id: "execute-1",
                      name: "execute",
                      result: "done",
                      isFailure: false,
                      providerExecuted: true,
                    } satisfies Response.StreamPartEncoded),
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
          Stream.runForEach((event) =>
            Ref.update(events, (current) => [...current, event]).pipe(
              Effect.andThen(
                event._tag === "ResponsePart" && event.part.type === "tool-call"
                  ? toolCallObserved.open
                  : Effect.void,
              ),
            ),
          ),
          Effect.forkScoped,
        );

        yield* agent.offer(new PromptInput({ message: userMessage("initial") }));
        yield* toolCallObserved.await;
        yield* agent.offer(new SteerInput({ message: userMessage("steer-now") }));
        yield* agent.offer(new QueueInput({ message: userMessage("later") }));
        expect(calls).toBe(1);
        yield* releaseToolResult.open;
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
        expect(
          emitted.some(
            (event) => event._tag === "ResponsePart" && event.part.type === "tool-result",
          ),
        ).toBe(true);
        expect(emitted.some((event) => event._tag === "ResponsePart")).toBe(true);
        expect(emitted.filter((event) => event._tag === "TurnCompleted")).toHaveLength(2);
      }),
    ),
  );

  it.effect("interrupts a stalled provider stream and preserves partial output for steering", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const partialObserved = yield* Latch.make();
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
                return Stream.concat(
                  Stream.fromIterable([
                    { type: "text-start", id: "partial" },
                    { type: "text-delta", id: "partial", delta: "partial answer" },
                  ] satisfies ReadonlyArray<Response.StreamPartEncoded>),
                  Stream.never,
                );
              }
              return Stream.fromIterable(textResponse("resumed answer"));
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("stalled"),
          layer: languageModelLayer,
        };
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000012"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "",
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
        yield* agent.events.pipe(
          Stream.runForEach((event) =>
            event._tag === "ResponsePart" && event.part.type === "text-delta"
              ? partialObserved.open
              : Effect.void,
          ),
          Effect.forkScoped,
        );

        yield* agent.offer(new PromptInput({ message: userMessage("initial") }));
        yield* partialObserved.await;
        yield* agent.offer(new SteerInput({ message: userMessage("steer-now") }));
        yield* agent.waitForIdle;

        expect(calls).toBe(2);
        expect(
          capturedPrompts[1]?.content.some((message) => promptText(message) === "partial answer"),
        ).toBe(true);
        expect(
          capturedPrompts[1]?.content.some((message) => promptText(message) === "steer-now"),
        ).toBe(true);
        const active = yield* store.activeMessages(session.id);
        expect(
          active.find((message) => promptText(message.message) === "partial answer")?.status,
        ).toBe("interrupted");
      }),
    ),
  );

  it.effect("treats compact without historical context as a completed no-op", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const languageModelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              calls += 1;
              return Stream.fromIterable([
                {
                  type: "tool-call",
                  id: "compact-1",
                  name: "compact",
                  params: {},
                  providerExecuted: false,
                },
                finishPart,
              ] satisfies ReadonlyArray<Response.StreamPartEncoded>);
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("compact-first"),
          layer: languageModelLayer,
        };
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000013"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "",
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
              compact: () => Effect.die("first-turn compaction must not run"),
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

        yield* agent.offer(new PromptInput({ message: userMessage("first") }));
        yield* agent.waitForIdle;

        expect(calls).toBe(1);
        expect(Option.isNone(yield* store.activeTurn(session.id))).toBe(true);
      }),
    ),
  );

  it.effect("marks a turn failed when generation fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const languageModelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.fail(
                AiError.make({
                  module: "AgentSessionTest",
                  method: "streamText",
                  reason: new AiError.InternalProviderError({ description: "failed" }),
                }),
              ),
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("failure"),
          layer: languageModelLayer,
        };
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000014"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "",
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
            Compactor.of({ compact: () => Effect.die("compaction was not expected") }),
          ),
          NodeCrypto.layer,
        );
        const context = yield* Layer.build(
          AgentSession.layer(session).pipe(Layer.provideMerge(dependencies)),
        );
        const agent = Context.get(context, AgentSession);
        const store = Context.get(context, SessionStore);
        yield* store.createSession(session);

        yield* agent.offer(new PromptInput({ message: userMessage("fail") }));
        yield* agent.waitForIdle;

        const messages = yield* store.activeMessages(session.id);
        const failedTurnId = Option.getOrThrow(Option.fromNullishOr(messages[0]?.turnId));
        const turn = yield* store.getTurn(failedTurnId);
        expect(Option.getOrThrow(turn).status).toBe("failed");
        expect(Option.isNone(yield* store.activeTurn(session.id))).toBe(true);
      }),
    ),
  );

  it.effect("opens before recovering a persisted steer as executable work", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const capturedPrompts: Array<Prompt.Prompt> = [];
        const languageModelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (options) => {
              capturedPrompts.push(options.prompt);
              return Stream.fromIterable(textResponse("recovered"));
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("recovery"),
          layer: languageModelLayer,
        };
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000015"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "",
            compactAtTokens: TokenLimit.make(100_000),
            summaryMaxTokens: TokenLimit.make(1_000),
            eventBufferSize: EventBufferSize.make(128),
          }),
          createdAt: TimestampMillis.make(1_700_000_000_000),
          updatedAt: TimestampMillis.make(1_700_000_000_000),
        });
        const storeContext = yield* Layer.build(SessionStore.layerMemory);
        const store = Context.get(storeContext, SessionStore);
        yield* store.createSession(session);
        yield* store.enqueue(
          new StoredPendingInput({
            id: PendingInputId.make("0198ee50-2c74-7000-8000-000000000016"),
            sessionId: session.id,
            kind: "steer",
            message: userMessage("recover-me"),
            createdAt: TimestampMillis.make(1_700_000_000_001),
          }),
        );
        const dependencies = Layer.mergeAll(
          Layer.succeed(SessionStore, store),
          Instructions.layer,
          TokenCounter.layer,
          ConversationModel.layer(binding),
          Layer.succeed(
            Compactor,
            Compactor.of({ compact: () => Effect.die("compaction was not expected") }),
          ),
          NodeCrypto.layer,
        );
        const context = yield* Layer.build(
          AgentSession.layer(session).pipe(Layer.provideMerge(dependencies)),
        );
        const agent = Context.get(context, AgentSession);
        const eventFiber = yield* agent.events.pipe(
          Stream.takeUntil((event) => event._tag === "SessionSettled"),
          Stream.runCollect,
          Effect.forkChild,
        );

        yield* agent.waitForIdle;
        const events = Array.from(yield* Fiber.join(eventFiber));

        expect(capturedPrompts).toHaveLength(1);
        expect(
          capturedPrompts[0]?.content.some((message) => promptText(message) === "recover-me"),
        ).toBe(true);
        expect(events[0]?._tag).toBe("SessionOpened");
        expect(events.some((event) => event._tag === "TurnStarted")).toBe(true);
        expect(yield* store.pendingCounts(session.id)).toEqual({ queue: 0, steer: 0 });
      }),
    ),
  );

  it.effect("settles a recovered turn whose final response was already committed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const languageModelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              calls += 1;
              return Stream.fromIterable(textResponse("duplicate"));
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("idempotent-recovery"),
          layer: languageModelLayer,
        };
        const timestamp = TimestampMillis.make(1_700_000_000_000);
        const session = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000017"),
          configuration: new SessionConfiguration({
            provider: binding.provider,
            model: binding.model,
            systemInstructions: "",
            compactAtTokens: TokenLimit.make(100_000),
            summaryMaxTokens: TokenLimit.make(1_000),
            eventBufferSize: EventBufferSize.make(128),
          }),
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const turn = new StoredTurn({
          id: TurnId.make("0198ee50-2c74-7000-8000-000000000018"),
          sessionId: session.id,
          status: "active",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const user = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000019"),
          sessionId: session.id,
          turnId: turn.id,
          sequence: MessageSequence.make(0),
          message: userMessage("original"),
          status: "complete",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const toolCall = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000020"),
          sessionId: session.id,
          turnId: turn.id,
          sequence: MessageSequence.make(1),
          message: Prompt.assistantMessage({
            content: [
              Prompt.toolCallPart({
                id: "execute-1",
                name: "execute",
                params: { command: "pwd" },
                providerExecuted: false,
              }),
            ],
          }),
          status: "complete",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const toolResult = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000021"),
          sessionId: session.id,
          turnId: turn.id,
          sequence: MessageSequence.make(2),
          message: Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: "execute-1",
                name: "execute",
                isFailure: false,
                result: "done",
              }),
            ],
          }),
          status: "complete",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const assistant = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000022"),
          sessionId: session.id,
          turnId: turn.id,
          sequence: MessageSequence.make(3),
          message: Prompt.assistantMessage({
            content: [Prompt.textPart({ text: "already committed" })],
          }),
          status: "complete",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const storeContext = yield* Layer.build(SessionStore.layerMemory);
        const store = Context.get(storeContext, SessionStore);
        yield* store.createSession(session);
        yield* store.createTurn(turn);
        yield* store.appendMessage(user, true);
        yield* store.appendMessage(toolCall, true);
        yield* store.appendMessage(toolResult, true);
        yield* store.appendMessage(assistant, true);
        const dependencies = Layer.mergeAll(
          Layer.succeed(SessionStore, store),
          Instructions.layer,
          TokenCounter.layer,
          ConversationModel.layer(binding),
          Layer.succeed(
            Compactor,
            Compactor.of({ compact: () => Effect.die("compaction was not expected") }),
          ),
          NodeCrypto.layer,
        );
        const context = yield* Layer.build(
          AgentSession.layer(session).pipe(Layer.provideMerge(dependencies)),
        );
        const agent = Context.get(context, AgentSession);

        yield* agent.waitForIdle;

        expect(calls).toBe(0);
        expect(Option.getOrThrow(yield* store.getTurn(turn.id)).status).toBe("completed");
        const active = yield* store.activeMessages(session.id);
        expect(
          active.filter((message) => promptText(message.message) === "already committed"),
        ).toHaveLength(1);

        const streamingSession = new StoredSession({
          id: SessionId.make("0198ee50-2c74-7000-8000-000000000023"),
          configuration: session.configuration,
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const streamingTurn = new StoredTurn({
          id: TurnId.make("0198ee50-2c74-7000-8000-000000000024"),
          sessionId: streamingSession.id,
          status: "active",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const streamingUser = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000025"),
          sessionId: streamingSession.id,
          turnId: streamingTurn.id,
          sequence: MessageSequence.make(0),
          message: userMessage("resume the checkpoint"),
          status: "complete",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        const streamingAssistant = new StoredMessage({
          id: MessageId.make("0198ee50-2c74-7000-8000-000000000026"),
          sessionId: streamingSession.id,
          turnId: streamingTurn.id,
          sequence: MessageSequence.make(1),
          message: Prompt.assistantMessage({
            content: [Prompt.textPart({ text: "checkpoint" })],
          }),
          status: "streaming",
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        yield* store.createSession(streamingSession);
        yield* store.createTurn(streamingTurn);
        yield* store.appendMessage(streamingUser, true);
        yield* store.appendMessage(streamingAssistant, true);
        const streamingContext = yield* Layer.build(
          AgentSession.layer(streamingSession).pipe(Layer.provideMerge(dependencies)),
        );
        const streamingAgent = Context.get(streamingContext, AgentSession);

        yield* streamingAgent.waitForIdle;

        expect(calls).toBe(1);
        expect(Option.getOrThrow(yield* store.getTurn(streamingTurn.id)).status).toBe("completed");
        const resumed = yield* store.activeMessages(streamingSession.id);
        expect(resumed.filter((message) => message.message.role === "assistant")).toHaveLength(2);
      }),
    ),
  );
});
