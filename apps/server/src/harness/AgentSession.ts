import {
  CompactionCompleted,
  CompactionStarted,
  type HarnessError,
  InputAccepted,
  InvalidSessionStateError,
  MessageCommitted,
  MessageStarted,
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
  type StoredMessageStatus,
  StoredPendingInput,
  type StoredSession,
  StoredTurn,
  type TokenCount,
  TurnCompleted,
  type TurnId,
  type TurnStatus,
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
import { makeMessageId, makePendingInputId, makeTurnId, now, tokenCount } from "./internal/Ids.ts";
import { TokenCounter } from "./internal/TokenCounter.ts";

interface GenerationGate {
  readonly signal: Deferred.Deferred<void>;
  readonly openToolCalls: ReadonlySet<string>;
  readonly requested: boolean;
}

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
        const recoveredActiveTurn = yield* store.activeTurn(session.id);
        const steerRequested = yield* Ref.make(recoveredPending.steer > 0);
        const generationGate = yield* Ref.make<Option.Option<GenerationGate>>(Option.none());
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

        const requestGenerationInterrupt = Effect.fn("AgentSession.requestGenerationInterrupt")(
          function* () {
            const signal = yield* Ref.modify(generationGate, (current) =>
              Option.match(current, {
                onNone: () => [Option.none<Deferred.Deferred<void>>(), current] as const,
                onSome: (gate) => {
                  const next = Option.some({ ...gate, requested: true });
                  return [
                    gate.openToolCalls.size === 0 ? Option.some(gate.signal) : Option.none(),
                    next,
                  ] as const;
                },
              }),
            );
            if (Option.isSome(signal)) yield* Deferred.succeed(signal.value, undefined);
          },
        );

        const trackToolCall = (part: Response.AnyPart) =>
          part.type !== "tool-call"
            ? Effect.void
            : Ref.update(generationGate, (current) =>
                Option.map(current, (gate) => ({
                  ...gate,
                  openToolCalls: new Set(gate.openToolCalls).add(part.id),
                })),
              );

        const shouldStopAfterPart = Effect.fn("AgentSession.shouldStopAfterPart")(function* (
          part: Response.AnyPart,
        ) {
          if (part.type !== "tool-result" || part.preliminary === true) return false;
          const current = yield* Ref.get(generationGate);
          if (Option.isNone(current) || !current.value.requested) return false;
          const remaining = new Set(current.value.openToolCalls);
          remaining.delete(part.id);
          return remaining.size === 0;
        });

        const finishGenerationPart = Effect.fn("AgentSession.finishGenerationPart")(function* (
          part: Response.AnyPart,
        ) {
          if (part.type !== "tool-result" || part.preliminary === true) return;
          const signal = yield* Ref.modify(generationGate, (current) =>
            Option.match(current, {
              onNone: () => [Option.none<Deferred.Deferred<void>>(), current] as const,
              onSome: (gate) => {
                const openToolCalls = new Set(gate.openToolCalls);
                openToolCalls.delete(part.id);
                return [
                  gate.requested && openToolCalls.size === 0
                    ? Option.some(gate.signal)
                    : Option.none(),
                  Option.some({ ...gate, openToolCalls }),
                ] as const;
              },
            }),
          );
          if (Option.isSome(signal)) yield* Deferred.succeed(signal.value, undefined);
        });

        const makeStoredMessage = Effect.fn("AgentSession.makeStoredMessage")(function* (
          turnId: TurnId,
          message: Prompt.Message,
          requestedId?: StoredMessage["id"],
          status: StoredMessageStatus = "complete",
        ) {
          const [messageId, sequence, timestamp] = yield* Effect.all([
            requestedId === undefined ? makeMessageId(crypto) : Effect.succeed(requestedId),
            store.nextMessageSequence(session.id),
            now,
          ]);
          return new StoredMessage({
            id: messageId,
            sessionId: session.id,
            turnId,
            sequence,
            message,
            status,
            createdAt: timestamp,
            updatedAt: timestamp,
          });
        });

        const publishMessageStarted = (stored: StoredMessage) =>
          publish(
            new MessageStarted({
              sessionId: session.id,
              turnId: stored.turnId,
              messageId: stored.id,
              role: stored.message.role,
            }),
          );

        const publishMessageCommitted = (stored: StoredMessage) =>
          publish(
            new MessageCommitted({
              sessionId: session.id,
              turnId: stored.turnId,
              messageId: stored.id,
              role: stored.message.role,
            }),
          );

        const appendMessage = Effect.fn("AgentSession.appendMessage")(
          function* (
            turnId: TurnId,
            message: Prompt.Message,
            options?: {
              readonly id?: StoredMessage["id"];
              readonly alreadyStarted?: boolean;
              readonly status?: StoredMessageStatus;
            },
          ) {
            const stored = yield* makeStoredMessage(turnId, message, options?.id, options?.status);
            if (options?.alreadyStarted !== true) {
              yield* publishMessageStarted(stored);
            }
            yield* store.appendMessage(stored, true);
            yield* publishMessageCommitted(stored);
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
          status: StoredMessageStatus,
        ) {
          const responsePrompt = Prompt.fromResponseParts(parts);
          let first = true;
          for (const message of responsePrompt.content) {
            yield* appendMessage(
              turnId,
              message,
              first ? { id: assistantMessageId, alreadyStarted: true, status } : { status },
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
            const interruptSignal = yield* Deferred.make<void>();
            yield* submitSemaphore.withPermits(1)(
              Effect.gen(function* () {
                const requested =
                  (yield* Ref.get(steerRequested)) || (yield* Ref.get(stopRequested));
                yield* Ref.set(
                  generationGate,
                  Option.some({
                    signal: interruptSignal,
                    openToolCalls: new Set<string>(),
                    requested,
                  }),
                );
                if (requested) yield* Deferred.succeed(interruptSignal, undefined);
              }),
            );
            const assistantMessageId = yield* makeMessageId(crypto);
            yield* publish(
              new MessageStarted({
                sessionId: session.id,
                turnId,
                messageId: assistantMessageId,
                role: "assistant",
              }),
            );
            const parts = yield* languageModel.streamText({ prompt, toolkit }).pipe(
              Stream.tap(trackToolCall),
              Stream.takeUntilEffect(shouldStopAfterPart),
              Stream.interruptWhen(Deferred.await(interruptSignal)),
              Stream.tap((part) =>
                finishGenerationPart(part).pipe(
                  Effect.andThen(
                    publish(
                      new ResponsePartEvent({
                        sessionId: session.id,
                        turnId,
                        messageId: assistantMessageId,
                        part,
                      }),
                    ),
                  ),
                ),
              ),
              Stream.runCollect,
              Effect.map((chunk) => preservePartialContent(Array.from(chunk))),
              Effect.ensuring(
                Ref.update(generationGate, (current) =>
                  Option.filter(current, (gate) => gate.signal !== interruptSignal),
                ),
              ),
            );
            const interrupted = yield* Deferred.isDone(interruptSignal);
            yield* setPhase("resolving-tools");
            yield* persistResponse(
              turnId,
              parts,
              assistantMessageId,
              interrupted ? "interrupted" : "complete",
            );
            return {
              compact: yield* Ref.get(compactionRequested),
              hasToolCalls: parts.some((part) => part.type === "tool-call"),
              hasNonCompactToolCalls: parts.some(
                (part) => part.type === "tool-call" && part.name !== "compact",
              ),
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

        const appendPendingInput = Effect.fn("AgentSession.appendPendingInput")(
          function* (turnId: TurnId, pending: StoredPendingInput) {
            const stored = yield* makeStoredMessage(turnId, pending.message);
            yield* publishMessageStarted(stored);
            yield* store.appendPendingMessage(pending.id, stored);
            yield* publishMessageCommitted(stored);
            return stored;
          },
          Effect.mapError((cause) =>
            Schema.is(SessionPersistenceError)(cause)
              ? cause
              : platformFailure("append a pending session message", cause),
          ),
        );

        const settleTurn = Effect.fn("AgentSession.settleTurn")(function* (
          turnId: TurnId,
          status: Extract<TurnStatus, "completed" | "interrupted">,
        ) {
          yield* setPhase("settling");
          yield* store.updateTurnStatus(turnId, status);
          yield* publish(new TurnCompleted({ sessionId: session.id, turnId }));
        });

        const continueTurn = Effect.fn("AgentSession.continueTurn")(
          function* (turnId: TurnId, initialLatestUser: StoredMessage) {
            let latestUser = initialLatestUser;
            yield* maybeCompact(latestUser.id, turnId);

            let running = true;
            while (running) {
              const result = yield* generate(turnId);
              if (yield* Ref.get(stopRequested)) {
                let discardedSteer = yield* store.peekPending(session.id, "steer");
                while (Option.isSome(discardedSteer)) {
                  yield* store.removePending(discardedSteer.value.id);
                  discardedSteer = yield* store.peekPending(session.id, "steer");
                }
                yield* Ref.set(steerRequested, false);
                yield* publishQueue();
                running = false;
                continue;
              }
              const steering = yield* store.peekPending(session.id, "steer");
              if (Option.isSome(steering)) {
                latestUser = yield* appendPendingInput(turnId, steering.value);
                const pendingAfterSteer = yield* store.pendingCounts(session.id);
                yield* Ref.set(steerRequested, pendingAfterSteer.steer > 0);
                yield* publishQueue();
                yield* maybeCompact(latestUser.id, turnId);
                continue;
              }
              yield* Ref.set(steerRequested, false);
              yield* publishQueue();
              if (result.compact) {
                const active = yield* store.activeMessages(session.id);
                const latestIndex = active.findIndex((message) => message.id === latestUser.id);
                if (latestIndex > 0) {
                  const tokens = yield* counter.count(yield* buildPrompt(active));
                  yield* runCompaction(active, latestUser.id, turnId, tokens);
                  continue;
                }
                if (!result.hasNonCompactToolCalls) {
                  running = false;
                  continue;
                }
              }
              if (result.hasToolCalls) continue;
              running = false;
            }

            const stopped = yield* Ref.getAndSet(stopRequested, false);
            yield* settleTurn(turnId, stopped ? "interrupted" : "completed");
          },
          (effect, turnId) =>
            Effect.onError(effect, () =>
              store.updateTurnStatus(turnId, "failed").pipe(Effect.ignore),
            ),
        );

        const runTurn = Effect.fn("AgentSession.runTurn")(function* (pending: StoredPendingInput) {
          const [turnId, timestamp] = yield* Effect.all([makeTurnId(crypto), now]);
          const turn = new StoredTurn({
            id: turnId,
            sessionId: session.id,
            status: "active",
            createdAt: timestamp,
            updatedAt: timestamp,
          });
          const latestUser = yield* makeStoredMessage(turnId, pending.message);
          yield* store.beginTurn(pending.id, turn, latestUser);
          if (pending.kind === "steer") {
            const remaining = yield* store.pendingCounts(session.id);
            yield* Ref.set(steerRequested, remaining.steer > 0);
          }
          yield* publish(new TurnStarted({ sessionId: session.id, turnId }));
          yield* publishMessageStarted(latestUser);
          yield* publishMessageCommitted(latestUser);
          yield* continueTurn(turnId, latestUser);
        });

        const resumeTurn = Effect.fn("AgentSession.resumeTurn")(
          function* (turn: StoredTurn) {
            yield* publish(new TurnStarted({ sessionId: session.id, turnId: turn.id }));
            const active = yield* store.activeMessages(session.id);
            let latestUserIndex = -1;
            for (let index = active.length - 1; index >= 0; index -= 1) {
              const message = active[index]!;
              if (message.turnId === turn.id && message.message.role === "user") {
                latestUserIndex = index;
                break;
              }
            }
            if (latestUserIndex < 0) {
              return yield* new InvalidSessionStateError({
                sessionId: session.id,
                phase: yield* Ref.get(phase),
                message: `Cannot recover active turn ${turn.id} without a user message`,
              });
            }

            const steering = yield* store.peekPending(session.id, "steer");
            if (Option.isSome(steering)) {
              const latestUser = yield* appendPendingInput(turn.id, steering.value);
              const pendingAfterSteer = yield* store.pendingCounts(session.id);
              yield* Ref.set(steerRequested, pendingAfterSteer.steer > 0);
              yield* publishQueue();
              yield* continueTurn(turn.id, latestUser);
              return;
            }

            const responseMessages = active
              .slice(latestUserIndex + 1)
              .filter((message) => message.turnId === turn.id);
            const latestResponse = responseMessages.at(-1);
            if (latestResponse?.status === "interrupted") {
              yield* settleTurn(turn.id, "interrupted");
              return;
            }

            const latestAssistantIsTerminal =
              latestResponse?.status === "complete" &&
              latestResponse?.message.role === "assistant" &&
              !latestResponse.message.content.some(
                (part) => part.type === "tool-call" || part.type === "tool-approval-request",
              );
            if (latestAssistantIsTerminal) {
              yield* settleTurn(turn.id, "completed");
              return;
            }

            yield* continueTurn(turn.id, active[latestUserIndex]!);
          },
          (effect, turn) =>
            Effect.onError(effect, () =>
              store.updateTurnStatus(turn.id, "failed").pipe(Effect.ignore),
            ),
        );

        const drain = Effect.fn("AgentSession.drain")(
          function* () {
            const activeTurn = yield* store.activeTurn(session.id);
            if (Option.isSome(activeTurn)) yield* resumeTurn(activeTurn.value);

            const recoveredSteer = yield* store.peekPending(session.id, "steer");
            if (Option.isSome(recoveredSteer)) {
              yield* publishQueue();
              yield* runTurn(recoveredSteer.value);
            }

            let next = yield* store.peekPending(session.id, "queue");
            while (Option.isSome(next)) {
              yield* publishQueue();
              yield* runTurn(next.value);
              next = yield* store.peekPending(session.id, "queue");
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
              yield* requestGenerationInterrupt();
              yield* publish(
                new InputAccepted({
                  sessionId: session.id,
                  inputId: null,
                  kind: "stop",
                }),
              );
              return;
            }

            const [id, timestamp] = yield* Effect.all([makePendingInputId(crypto), now]);
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
            if (kind === "steer") {
              yield* Ref.set(steerRequested, true);
              yield* requestGenerationInterrupt();
            }
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
        const hasRecoveredWork =
          Option.isSome(recoveredActiveTurn) ||
          recoveredPending.queue > 0 ||
          recoveredPending.steer > 0;
        if (hasRecoveredWork) yield* setPhase("checking-tokens");
        yield* Effect.forkScoped(driver);
        if (hasRecoveredWork) yield* Queue.offer(wake, undefined);

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
