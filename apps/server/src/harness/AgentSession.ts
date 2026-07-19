import {
  CompactionCompleted,
  CompactionStarted,
  type HarnessError,
  InputAccepted,
  InvalidSessionStateError,
  MessageCommitted,
  MessageId,
  MessageStarted,
  PendingInputId,
  PhaseChanged,
  QueueChanged,
  ResponsePartEvent,
  SessionBusyError,
  type SessionEvent,
  SessionFailed,
  SessionOpened,
  type SessionId,
  type SessionInput,
  type SessionPhase,
  SessionModelError,
  SessionPersistenceError,
  SessionSettled,
  StoredMessage,
  StoredPendingInput,
  type StoredSession,
  StoredTurn,
  type TokenCount,
  TurnCompleted,
  TurnId,
  TurnStarted,
} from "@compass/contracts";
import {
  Context,
  Crypto,
  Deferred,
  Effect,
  Layer,
  Option,
  PubSub,
  Queue,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { LanguageModel, Prompt, Response } from "effect/unstable/ai";
import { Compactor } from "./Compaction.ts";
import { Instructions } from "./Instructions.ts";
import { ConversationModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { makeToolkit } from "./Tools.ts";
import { now, tokenCount } from "./internal/Ids.ts";
import { TokenCounter } from "./internal/TokenCounter.ts";

export class AgentSession extends Context.Service<
  AgentSession,
  {
    readonly id: SessionId;
    readonly offer: (input: SessionInput) => Effect.Effect<void, HarnessError>;
    readonly events: Stream.Stream<SessionEvent>;
    readonly run: (input: Stream.Stream<SessionInput>) => Stream.Stream<SessionEvent>;
    readonly waitForIdle: Effect.Effect<void>;
  }
>()("@compass/server/harness/AgentSession") {
  static readonly layer = (session: StoredSession) =>
    Layer.effect(
      AgentSession,
      Effect.gen(function* () {
        const store = yield* SessionStore;
        const instructions = yield* Instructions;
        const compactor = yield* Compactor;
        const counter = yield* TokenCounter;
        const binding = yield* ConversationModel;
        const crypto = yield* Crypto.Crypto;
        const modelContext = yield* Layer.build(binding.layer);
        const languageModel = Context.get(modelContext, LanguageModel.LanguageModel);
        const phase = yield* Ref.make<SessionPhase>("idle");
        const initiallyIdle = yield* Deferred.make<void>();
        yield* Deferred.succeed(initiallyIdle, undefined);
        const idleSignal = yield* Ref.make(initiallyIdle);
        const stopRequested = yield* Ref.make(false);
        const recoveredPending = yield* store.pendingCounts(session.id);
        const steerRequested = yield* Ref.make(recoveredPending.steer > 0);
        const wake = yield* Queue.sliding<void>(1);
        const eventBus = yield* PubSub.bounded<SessionEvent>({
          capacity: session.configuration.eventBufferSize,
          replay: Math.min(session.configuration.eventBufferSize, 32),
        });
        const submitSemaphore = yield* Semaphore.make(1);

        yield* Effect.addFinalizer(() =>
          Effect.all([Queue.shutdown(wake), PubSub.shutdown(eventBus)]).pipe(Effect.asVoid),
        );

        const publish = (event: SessionEvent) =>
          PubSub.publish(eventBus, event).pipe(Effect.asVoid);

        const setPhase = Effect.fn("AgentSession.setPhase")(function* (next: SessionPhase) {
          const current = yield* Ref.get(phase);
          if (current === "idle" && next !== "idle") {
            yield* Ref.set(idleSignal, yield* Deferred.make<void>());
          }
          yield* Ref.set(phase, next);
          yield* publish(new PhaseChanged({ sessionId: session.id, phase: next }));
          if (next === "idle") {
            yield* Deferred.succeed(yield* Ref.get(idleSignal), undefined);
          }
        });

        const publishQueue = Effect.fn("AgentSession.publishQueue")(function* () {
          const counts = yield* store.pendingCounts(session.id);
          yield* publish(
            new QueueChanged({
              sessionId: session.id,
              queued: tokenCount(counts.queue),
              steering: tokenCount(counts.steer),
            }),
          );
        });

        const platformFailure = (operation: string, cause: unknown) =>
          new SessionPersistenceError({
            operation,
            message: `Could not ${operation}`,
            cause,
          });

        const appendMessage = Effect.fn("AgentSession.appendMessage")(
          function* (
            turnId: TurnId,
            message: Prompt.Message,
            options?: {
              readonly id?: StoredMessage["id"];
              readonly alreadyStarted?: boolean;
            },
          ) {
            const [id, sequence, timestamp] = yield* Effect.all([
              options?.id === undefined
                ? Effect.map(crypto.randomUUIDv7, (value) => MessageId.make(value))
                : Effect.succeed(options.id),
              store.nextMessageSequence(session.id),
              now,
            ]);
            const stored = new StoredMessage({
              id,
              sessionId: session.id,
              turnId,
              sequence,
              message,
              status: "complete",
              createdAt: timestamp,
              updatedAt: timestamp,
            });
            if (options?.alreadyStarted !== true) {
              yield* publish(
                new MessageStarted({
                  sessionId: session.id,
                  turnId,
                  messageId: id,
                  role: message.role,
                }),
              );
            }
            yield* store.appendMessage(stored, true);
            yield* publish(
              new MessageCommitted({
                sessionId: session.id,
                turnId,
                messageId: id,
                role: message.role,
              }),
            );
            return stored;
          },
          Effect.mapError((cause) =>
            Schema.is(SessionPersistenceError)(cause)
              ? cause
              : platformFailure("append a session message", cause),
          ),
        );

        const buildPrompt = Effect.fn("AgentSession.buildPrompt")(function* (
          active: ReadonlyArray<StoredMessage>,
        ) {
          const system = yield* instructions.build(session);
          return Prompt.concat(system, Prompt.fromMessages(active.map((stored) => stored.message)));
        });

        const runCompaction = Effect.fn("AgentSession.runCompaction")(function* (
          active: ReadonlyArray<StoredMessage>,
          latestUserMessageId: StoredMessage["id"],
          turnId: TurnId,
          tokensBefore: TokenCount,
        ) {
          yield* setPhase("compacting");
          yield* publish(new CompactionStarted({ sessionId: session.id, tokensBefore }));
          const summary = yield* compactor.compact(
            session,
            active,
            latestUserMessageId,
            turnId,
            tokensBefore,
          );
          yield* publish(
            new CompactionCompleted({
              sessionId: session.id,
              summaryMessageId: summary.id,
              tokensBefore,
            }),
          );
        });

        const maybeCompact = Effect.fn("AgentSession.maybeCompact")(function* (
          latestUserMessageId: StoredMessage["id"],
          turnId: TurnId,
        ) {
          yield* setPhase("checking-tokens");
          const active = yield* store.activeMessages(session.id);
          const latestIndex = active.findIndex((message) => message.id === latestUserMessageId);
          if (latestIndex <= 0) return false;
          const tokens = yield* counter.count(yield* buildPrompt(active));
          if (tokens < session.configuration.compactAtTokens) return false;
          yield* runCompaction(active, latestUserMessageId, turnId, tokens);
          return true;
        });

        const persistResponse = Effect.fn("AgentSession.persistResponse")(function* (
          turnId: TurnId,
          parts: ReadonlyArray<Response.AnyPart>,
          assistantMessageId: StoredMessage["id"],
        ) {
          const responsePrompt = Prompt.fromResponseParts(parts);
          let first = true;
          for (const message of responsePrompt.content) {
            yield* appendMessage(
              turnId,
              message,
              first ? { id: assistantMessageId, alreadyStarted: true } : undefined,
            );
            first = false;
          }
        });

        const preservePartialContent = (
          parts: ReadonlyArray<Response.AnyPart>,
        ): ReadonlyArray<Response.AnyPart> => {
          const text = new Set<string>();
          const reasoning = new Set<string>();
          for (const part of parts) {
            if (part.type === "text-start") text.add(part.id);
            if (part.type === "text-end") text.delete(part.id);
            if (part.type === "reasoning-start") reasoning.add(part.id);
            if (part.type === "reasoning-end") reasoning.delete(part.id);
          }
          return [
            ...parts,
            ...Array.from(text, (id) => Response.makePart("text-end", { id })),
            ...Array.from(reasoning, (id) => Response.makePart("reasoning-end", { id })),
          ];
        };

        const generate = Effect.fn("AgentSession.generate")(
          function* (turnId: TurnId) {
            yield* setPhase("generating");
            const active = yield* store.activeMessages(session.id);
            const prompt = yield* buildPrompt(active);
            const compactionRequested = yield* Ref.make(false);
            const toolkit = yield* makeToolkit(compactionRequested);
            const openToolCalls = yield* Ref.make(new Set<string>());
            const assistantMessageId = yield* crypto.randomUUIDv7.pipe(
              Effect.map((value) => MessageId.make(value)),
            );
            yield* publish(
              new MessageStarted({
                sessionId: session.id,
                turnId,
                messageId: assistantMessageId,
                role: "assistant",
              }),
            );
            const parts = yield* languageModel.streamText({ prompt, toolkit }).pipe(
              Stream.tap((part) =>
                publish(
                  new ResponsePartEvent({
                    sessionId: session.id,
                    turnId,
                    messageId: assistantMessageId,
                    part,
                  }),
                ),
              ),
              Stream.takeUntilEffect((part) =>
                Effect.gen(function* () {
                  if (part.type === "tool-call") {
                    yield* Ref.update(openToolCalls, (current) => new Set(current).add(part.id));
                  } else if (part.type === "tool-result" && part.preliminary !== true) {
                    yield* Ref.update(openToolCalls, (current) => {
                      const next = new Set(current);
                      next.delete(part.id);
                      return next;
                    });
                  }
                  const interruptRequested =
                    (yield* Ref.get(steerRequested)) || (yield* Ref.get(stopRequested));
                  if (!interruptRequested) return false;
                  if ((yield* Ref.get(openToolCalls)).size > 0) return false;
                  return (
                    part.type === "text-delta" ||
                    part.type === "text-end" ||
                    part.type === "reasoning-delta" ||
                    part.type === "reasoning-end" ||
                    part.type === "tool-result" ||
                    part.type === "finish"
                  );
                }),
              ),
              Stream.runCollect,
              Effect.map((chunk) => preservePartialContent(Array.from(chunk))),
            );
            yield* setPhase("resolving-tools");
            yield* persistResponse(turnId, parts, assistantMessageId);
            return {
              compact: yield* Ref.get(compactionRequested),
              hasToolCalls: parts.some((part) => part.type === "tool-call"),
            };
          },
          (effect) =>
            Effect.mapError(
              effect,
              (cause) =>
                new SessionModelError({
                  sessionId: session.id,
                  message: "The conversation model failed",
                  cause,
                }),
            ),
        );

        const runTurn = Effect.fn("AgentSession.runTurn")(function* (pending: StoredPendingInput) {
          const [turnId, timestamp] = yield* Effect.all([
            Effect.map(crypto.randomUUIDv7, (value) => TurnId.make(value)),
            now,
          ]);
          const turn = new StoredTurn({
            id: turnId,
            sessionId: session.id,
            status: "active",
            createdAt: timestamp,
            updatedAt: timestamp,
          });
          yield* store.createTurn(turn);
          yield* publish(new TurnStarted({ sessionId: session.id, turnId }));
          let latestUser = yield* appendMessage(turnId, pending.message);
          yield* maybeCompact(latestUser.id, turnId);

          let running = true;
          while (running) {
            const result = yield* generate(turnId);
            if (yield* Ref.get(stopRequested)) {
              let discardedSteer = yield* store.takePending(session.id, "steer");
              while (Option.isSome(discardedSteer)) {
                discardedSteer = yield* store.takePending(session.id, "steer");
              }
              yield* Ref.set(steerRequested, false);
              yield* publishQueue();
              running = false;
              continue;
            }
            const steering = yield* store.takePending(session.id, "steer");
            const pendingAfterSteer = yield* store.pendingCounts(session.id);
            yield* Ref.set(steerRequested, pendingAfterSteer.steer > 0);
            yield* publishQueue();
            if (Option.isSome(steering)) {
              latestUser = yield* appendMessage(turnId, steering.value.message);
              yield* maybeCompact(latestUser.id, turnId);
              continue;
            }
            if (result.compact) {
              const active = yield* store.activeMessages(session.id);
              const latestIndex = active.findIndex((message) => message.id === latestUser.id);
              if (latestIndex > 0) {
                const tokens = yield* counter.count(yield* buildPrompt(active));
                yield* runCompaction(active, latestUser.id, turnId, tokens);
              }
              continue;
            }
            if (result.hasToolCalls) continue;
            running = false;
          }

          yield* setPhase("settling");
          const stopped = yield* Ref.getAndSet(stopRequested, false);
          yield* store.updateTurnStatus(turnId, stopped ? "interrupted" : "completed");
          yield* publish(new TurnCompleted({ sessionId: session.id, turnId }));
        });

        const drain = Effect.fn("AgentSession.drain")(
          function* () {
            let next = yield* store.takePending(session.id, "queue");
            while (Option.isSome(next)) {
              yield* publishQueue();
              yield* runTurn(next.value);
              next = yield* store.takePending(session.id, "queue");
            }
            yield* setPhase("idle");
            yield* publish(new SessionSettled({ sessionId: session.id }));
          },
          Effect.catch((cause) =>
            Effect.gen(function* () {
              yield* setPhase("failed");
              yield* publish(
                new SessionFailed({
                  sessionId: session.id,
                  message: Schema.is(SessionModelError)(cause)
                    ? `${cause.message}: ${String(cause.cause)}`
                    : String(cause),
                }),
              );
              yield* setPhase("idle");
              yield* publish(new SessionSettled({ sessionId: session.id }));
            }),
          ),
        );

        const driver = Queue.take(wake).pipe(Effect.andThen(drain()), Effect.forever);
        yield* Effect.forkScoped(driver);
        if (recoveredPending.queue > 0 || recoveredPending.steer > 0) {
          yield* Queue.offer(wake, undefined);
        }

        const offer = Effect.fn("AgentSession.offer")(
          function* (input: SessionInput) {
            const currentPhase = yield* Ref.get(phase);
            if (input._tag === "Prompt" && currentPhase !== "idle") {
              return yield* new SessionBusyError({ sessionId: session.id });
            }
            if (input._tag === "Steer" && currentPhase === "idle") {
              return yield* new InvalidSessionStateError({
                sessionId: session.id,
                phase: currentPhase,
                message: "Cannot steer an idle session",
              });
            }
            if (input._tag === "Stop" && currentPhase === "idle") {
              return yield* new InvalidSessionStateError({
                sessionId: session.id,
                phase: currentPhase,
                message: "Cannot stop an idle session",
              });
            }
            if (input._tag === "Stop") {
              yield* Ref.set(stopRequested, true);
              yield* publish(
                new InputAccepted({
                  sessionId: session.id,
                  inputId: null,
                  kind: "stop",
                }),
              );
              return;
            }

            const [id, timestamp] = yield* Effect.all([
              Effect.map(crypto.randomUUIDv7, (value) => PendingInputId.make(value)),
              now,
            ]);
            const kind = input._tag === "Steer" ? "steer" : "queue";
            yield* store.enqueue(
              new StoredPendingInput({
                id,
                sessionId: session.id,
                kind,
                message: input.message,
                createdAt: timestamp,
              }),
            );
            if (kind === "steer") yield* Ref.set(steerRequested, true);
            if (currentPhase === "idle") yield* setPhase("checking-tokens");
            yield* publish(
              new InputAccepted({
                sessionId: session.id,
                inputId: id,
                kind:
                  input._tag === "Prompt" ? "prompt" : input._tag === "Queue" ? "queue" : "steer",
              }),
            );
            yield* publishQueue();
            yield* Queue.offer(wake, undefined);
          },
          submitSemaphore.withPermits(1),
          (effect) =>
            Effect.mapError(effect, (cause) =>
              Schema.is(SessionBusyError)(cause) ||
              Schema.is(InvalidSessionStateError)(cause) ||
              Schema.is(SessionPersistenceError)(cause)
                ? cause
                : platformFailure("accept session input", cause),
            ),
        );

        const events = Stream.fromPubSub(eventBus);
        const run = (input: Stream.Stream<SessionInput>) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* input.pipe(
                Stream.runForEach((command) =>
                  offer(command).pipe(
                    Effect.catch((cause) =>
                      publish(
                        new SessionFailed({
                          sessionId: session.id,
                          message: String(cause),
                        }),
                      ),
                    ),
                  ),
                ),
                Effect.forkScoped,
              );
              return events;
            }),
          );

        const waitForIdle = Effect.suspend(() =>
          Ref.get(idleSignal).pipe(Effect.flatMap(Deferred.await)),
        );

        yield* publish(new SessionOpened({ sessionId: session.id }));

        return AgentSession.of({
          id: session.id,
          offer,
          events,
          run,
          waitForIdle,
        });
      }),
    );
}
