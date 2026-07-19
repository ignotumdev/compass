import {
  type ContextPosition,
  type MessageId,
  type MessageSequence,
  PendingCounts,
  type PendingInputKind,
  type SessionId,
  SessionNotFoundError,
  SessionPersistenceError,
  type StoredMessage,
  type StoredPendingInput,
  type StoredSession,
  StoredTurn,
  type TurnId,
  type TurnStatus,
  TokenCount,
} from "@compass/contracts";
import { Context, Effect, Layer, Option, Ref } from "effect";

export interface SessionStoreService {
  readonly createSession: (session: StoredSession) => Effect.Effect<void, SessionPersistenceError>;
  readonly getSession: (
    sessionId: SessionId,
  ) => Effect.Effect<Option.Option<StoredSession>, SessionPersistenceError>;
  readonly createTurn: (turn: StoredTurn) => Effect.Effect<void, SessionPersistenceError>;
  readonly updateTurnStatus: (
    turnId: TurnId,
    status: TurnStatus,
  ) => Effect.Effect<void, SessionPersistenceError>;
  readonly appendMessage: (
    message: StoredMessage,
    active: boolean,
  ) => Effect.Effect<void, SessionPersistenceError>;
  readonly nextMessageSequence: (
    sessionId: SessionId,
  ) => Effect.Effect<MessageSequence, SessionPersistenceError>;
  readonly activeMessages: (
    sessionId: SessionId,
  ) => Effect.Effect<ReadonlyArray<StoredMessage>, SessionPersistenceError>;
  readonly replaceActiveContext: (
    sessionId: SessionId,
    messageIds: ReadonlyArray<MessageId>,
  ) => Effect.Effect<void, SessionPersistenceError>;
  readonly commitCompaction: (
    summary: StoredMessage,
    latestUserMessageId: MessageId,
  ) => Effect.Effect<void, SessionPersistenceError>;
  readonly enqueue: (input: StoredPendingInput) => Effect.Effect<void, SessionPersistenceError>;
  readonly takePending: (
    sessionId: SessionId,
    kind: PendingInputKind,
  ) => Effect.Effect<Option.Option<StoredPendingInput>, SessionPersistenceError>;
  readonly pendingCounts: (
    sessionId: SessionId,
  ) => Effect.Effect<PendingCounts, SessionPersistenceError>;
}

export class SessionStore extends Context.Service<SessionStore, SessionStoreService>()(
  "@compass/server/harness/SessionStore",
) {
  static readonly layerMemory = Layer.effect(
    SessionStore,
    Effect.gen(function* () {
      const state = yield* Ref.make<MemoryState>({
        sessions: new Map(),
        turns: new Map(),
        messages: new Map(),
        context: new Map(),
        pending: new Map(),
      });

      const createSession = Effect.fn("SessionStore.memory.createSession")(function* (
        session: StoredSession,
      ) {
        yield* Ref.update(state, (current) => ({
          ...current,
          sessions: new Map(current.sessions).set(session.id, session),
          context: new Map(current.context).set(session.id, []),
          pending: new Map(current.pending).set(session.id, []),
        }));
      });

      const getSession = Effect.fn("SessionStore.memory.getSession")(function* (
        sessionId: SessionId,
      ) {
        const current = yield* Ref.get(state);
        return Option.fromNullishOr(current.sessions.get(sessionId));
      });

      const createTurn = Effect.fn("SessionStore.memory.createTurn")(function* (turn: StoredTurn) {
        yield* Ref.update(state, (current) => ({
          ...current,
          turns: new Map(current.turns).set(turn.id, turn),
        }));
      });

      const updateTurnStatus = Effect.fn("SessionStore.memory.updateTurnStatus")(function* (
        turnId: TurnId,
        status: TurnStatus,
      ) {
        yield* Ref.update(state, (current) => {
          const turn = current.turns.get(turnId);
          if (turn === undefined) return current;
          return {
            ...current,
            turns: new Map(current.turns).set(
              turnId,
              new StoredTurn({
                id: turn.id,
                sessionId: turn.sessionId,
                status,
                createdAt: turn.createdAt,
                updatedAt: turn.updatedAt,
              }),
            ),
          };
        });
      });

      const appendMessage = Effect.fn("SessionStore.memory.appendMessage")(function* (
        message: StoredMessage,
        active: boolean,
      ) {
        yield* Ref.update(state, (current) => {
          const messages = new Map(current.messages).set(message.id, message);
          if (!active) return { ...current, messages };
          const activeIds = current.context.get(message.sessionId) ?? [];
          return {
            ...current,
            messages,
            context: new Map(current.context).set(message.sessionId, [...activeIds, message.id]),
          };
        });
      });

      const activeMessages = Effect.fn("SessionStore.memory.activeMessages")(
        function* (sessionId: SessionId) {
          const current = yield* Ref.get(state);
          const session = current.sessions.get(sessionId);
          if (session === undefined) return yield* new SessionNotFoundError({ sessionId });
          const ids = current.context.get(sessionId) ?? [];
          return ids.flatMap((id) => {
            const message = current.messages.get(id);
            return message === undefined ? [] : [message];
          });
        },
        Effect.mapError(
          (error) =>
            new SessionPersistenceError({
              operation: "activeMessages",
              message: "Could not load active session messages",
              cause: error,
            }),
        ),
      );

      const nextMessageSequence = Effect.fn("SessionStore.memory.nextMessageSequence")(function* (
        sessionId: SessionId,
      ) {
        const current = yield* Ref.get(state);
        let maximum = -1;
        for (const message of current.messages.values()) {
          if (message.sessionId === sessionId && message.sequence > maximum) {
            maximum = message.sequence;
          }
        }
        return (maximum + 1) as MessageSequence;
      });

      const replaceActiveContext = Effect.fn("SessionStore.memory.replaceActiveContext")(function* (
        sessionId: SessionId,
        messageIds: ReadonlyArray<MessageId>,
      ) {
        yield* Ref.update(state, (current) => ({
          ...current,
          context: new Map(current.context).set(sessionId, [...messageIds]),
        }));
      });

      const commitCompaction = Effect.fn("SessionStore.memory.commitCompaction")(function* (
        summary: StoredMessage,
        latestUserMessageId: MessageId,
      ) {
        yield* Ref.update(state, (current) => ({
          ...current,
          messages: new Map(current.messages).set(summary.id, summary),
          context: new Map(current.context).set(summary.sessionId, [
            summary.id,
            latestUserMessageId,
          ]),
        }));
      });

      const enqueue = Effect.fn("SessionStore.memory.enqueue")(function* (
        input: StoredPendingInput,
      ) {
        yield* Ref.update(state, (current) => ({
          ...current,
          pending: new Map(current.pending).set(input.sessionId, [
            ...(current.pending.get(input.sessionId) ?? []),
            input,
          ]),
        }));
      });

      const takePending = Effect.fn("SessionStore.memory.takePending")(function* (
        sessionId: SessionId,
        kind: PendingInputKind,
      ) {
        return yield* Ref.modify(state, (current) => {
          const pending = current.pending.get(sessionId) ?? [];
          const index = pending.findIndex((input) => input.kind === kind);
          if (index < 0) return [Option.none(), current] as const;
          const input = pending[index]!;
          const nextPending = [...pending];
          nextPending.splice(index, 1);
          return [
            Option.some(input),
            {
              ...current,
              pending: new Map(current.pending).set(sessionId, nextPending),
            },
          ] as const;
        });
      });

      const pendingCounts = Effect.fn("SessionStore.memory.pendingCounts")(function* (
        sessionId: SessionId,
      ) {
        const current = yield* Ref.get(state);
        const pending = current.pending.get(sessionId) ?? [];
        return new PendingCounts({
          queue: TokenCount.make(pending.filter((input) => input.kind === "queue").length),
          steer: TokenCount.make(pending.filter((input) => input.kind === "steer").length),
        });
      });

      return SessionStore.of({
        createSession,
        getSession,
        createTurn,
        updateTurnStatus,
        appendMessage,
        nextMessageSequence,
        activeMessages,
        replaceActiveContext,
        commitCompaction,
        enqueue,
        takePending,
        pendingCounts,
      });
    }),
  );
}

interface MemoryState {
  readonly sessions: ReadonlyMap<SessionId, StoredSession>;
  readonly turns: ReadonlyMap<TurnId, StoredTurn>;
  readonly messages: ReadonlyMap<MessageId, StoredMessage>;
  readonly context: ReadonlyMap<SessionId, ReadonlyArray<MessageId>>;
  readonly pending: ReadonlyMap<SessionId, ReadonlyArray<StoredPendingInput>>;
}

export const toContextPositions = (
  messageIds: ReadonlyArray<MessageId>,
): ReadonlyArray<readonly [MessageId, ContextPosition]> =>
  messageIds.map((id, position) => [id, position as ContextPosition] as const);
