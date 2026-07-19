import {
  type ContextPosition,
  type MessageId,
  type MessageSequence,
  type PendingInputId,
  PendingCounts,
  type PendingInputKind,
  type SessionId,
  SessionNotFoundError,
  SessionPersistenceError,
  type StoredMessage,
  type StoredPendingInput,
  type StoredSession,
  StoredTurn,
  TimestampMillis,
  type TurnId,
  type TurnStatus,
  TokenCount,
} from "@compass/contracts";
import { Clock, Context, Effect, Equal, Layer, Option, Ref } from "effect";

export interface SessionStoreService {
  readonly createSession: (session: StoredSession) => Effect.Effect<void, SessionPersistenceError>;
  readonly getSession: (
    sessionId: SessionId,
  ) => Effect.Effect<Option.Option<StoredSession>, SessionPersistenceError>;
  readonly createTurn: (turn: StoredTurn) => Effect.Effect<void, SessionPersistenceError>;
  readonly getTurn: (
    turnId: TurnId,
  ) => Effect.Effect<Option.Option<StoredTurn>, SessionPersistenceError>;
  readonly activeTurn: (
    sessionId: SessionId,
  ) => Effect.Effect<Option.Option<StoredTurn>, SessionPersistenceError>;
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
  readonly peekPending: (
    sessionId: SessionId,
    kind: PendingInputKind,
  ) => Effect.Effect<Option.Option<StoredPendingInput>, SessionPersistenceError>;
  readonly removePending: (inputId: PendingInputId) => Effect.Effect<void, SessionPersistenceError>;
  readonly beginTurn: (
    inputId: PendingInputId,
    turn: StoredTurn,
    firstMessage: StoredMessage,
  ) => Effect.Effect<void, SessionPersistenceError>;
  readonly appendPendingMessage: (
    inputId: PendingInputId,
    message: StoredMessage,
  ) => Effect.Effect<void, SessionPersistenceError>;
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
        const created = yield* Ref.modify(state, (current) =>
          current.sessions.has(session.id)
            ? ([false, current] as const)
            : ([
                true,
                {
                  ...current,
                  sessions: new Map(current.sessions).set(session.id, session),
                  context: new Map(current.context).set(session.id, []),
                  pending: new Map(current.pending).set(session.id, []),
                },
              ] as const),
        );
        if (!created) {
          return yield* new SessionPersistenceError({
            operation: "createSession",
            message: `Session ${session.id} already exists`,
            cause: new Error("Duplicate session identifier"),
          });
        }
      });

      const getSession = Effect.fn("SessionStore.memory.getSession")(function* (
        sessionId: SessionId,
      ) {
        const current = yield* Ref.get(state);
        return Option.fromNullishOr(current.sessions.get(sessionId));
      });

      const createTurn = Effect.fn("SessionStore.memory.createTurn")(function* (turn: StoredTurn) {
        const created = yield* Ref.modify(state, (current) => {
          const hasActiveTurn = [...current.turns.values()].some(
            (existing) => existing.sessionId === turn.sessionId && existing.status === "active",
          );
          if (current.turns.has(turn.id) || (turn.status === "active" && hasActiveTurn)) {
            return [false, current] as const;
          }
          return [true, { ...current, turns: new Map(current.turns).set(turn.id, turn) }] as const;
        });
        if (!created) {
          return yield* new SessionPersistenceError({
            operation: "createTurn",
            message: `Turn ${turn.id} conflicts with an existing turn`,
            cause: new Error("Duplicate or concurrently active turn"),
          });
        }
      });

      const getTurn = Effect.fn("SessionStore.memory.getTurn")(function* (turnId: TurnId) {
        const current = yield* Ref.get(state);
        return Option.fromNullishOr(current.turns.get(turnId));
      });

      const activeTurn = Effect.fn("SessionStore.memory.activeTurn")(function* (
        sessionId: SessionId,
      ) {
        const current = yield* Ref.get(state);
        return Option.fromNullishOr(
          [...current.turns.values()]
            .filter((turn) => turn.sessionId === sessionId && turn.status === "active")
            .toSorted(
              (left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id),
            )[0],
        );
      });

      const updateTurnStatus = Effect.fn("SessionStore.memory.updateTurnStatus")(function* (
        turnId: TurnId,
        status: TurnStatus,
      ) {
        const updatedAt = TimestampMillis.make(yield* Clock.currentTimeMillis);
        const updated = yield* Ref.modify(state, (current) => {
          const turn = current.turns.get(turnId);
          if (turn === undefined) return [true, current] as const;
          const conflictingActiveTurn =
            status === "active" &&
            [...current.turns.values()].some(
              (existing) =>
                existing.id !== turn.id &&
                existing.sessionId === turn.sessionId &&
                existing.status === "active",
            );
          if (conflictingActiveTurn) return [false, current] as const;
          return [
            true,
            {
              ...current,
              turns: new Map(current.turns).set(
                turnId,
                new StoredTurn({
                  id: turn.id,
                  sessionId: turn.sessionId,
                  status,
                  createdAt: turn.createdAt,
                  updatedAt,
                }),
              ),
            },
          ] as const;
        });
        if (!updated) {
          return yield* new SessionPersistenceError({
            operation: "updateTurnStatus",
            message: `Turn ${turnId} conflicts with the active turn for its session`,
            cause: new Error("Concurrently active turn"),
          });
        }
      });

      const appendMessage = Effect.fn("SessionStore.memory.appendMessage")(function* (
        message: StoredMessage,
        active: boolean,
      ) {
        const appended = yield* Ref.modify(state, (current) => {
          if (current.messages.has(message.id)) return [false, current] as const;
          const messages = new Map(current.messages).set(message.id, message);
          if (!active) return [true, { ...current, messages }] as const;
          const activeIds = current.context.get(message.sessionId) ?? [];
          return [
            true,
            {
              ...current,
              messages,
              context: new Map(current.context).set(message.sessionId, [...activeIds, message.id]),
            },
          ] as const;
        });
        if (!appended) {
          return yield* new SessionPersistenceError({
            operation: "appendMessage",
            message: `Message ${message.id} already exists`,
            cause: new Error("Duplicate message identifier"),
          });
        }
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
        const committed = yield* Ref.modify(state, (current) => {
          const latest = current.messages.get(latestUserMessageId);
          if (
            current.messages.has(summary.id) ||
            latest === undefined ||
            latest.sessionId !== summary.sessionId
          ) {
            return [false, current] as const;
          }
          return [
            true,
            {
              ...current,
              messages: new Map(current.messages).set(summary.id, summary),
              context: new Map(current.context).set(summary.sessionId, [
                summary.id,
                latestUserMessageId,
              ]),
            },
          ] as const;
        });
        if (!committed) {
          return yield* new SessionPersistenceError({
            operation: "commitCompaction",
            message: `Could not commit summary message ${summary.id}`,
            cause: new Error("Duplicate summary or invalid latest user message"),
          });
        }
      });

      const enqueue = Effect.fn("SessionStore.memory.enqueue")(function* (
        input: StoredPendingInput,
      ) {
        const enqueued = yield* Ref.modify(state, (current) => {
          const duplicate = [...current.pending.values()].some((inputs) =>
            inputs.some((existing) => existing.id === input.id),
          );
          if (duplicate || !current.sessions.has(input.sessionId)) {
            return [false, current] as const;
          }
          return [
            true,
            {
              ...current,
              pending: new Map(current.pending).set(input.sessionId, [
                ...(current.pending.get(input.sessionId) ?? []),
                input,
              ]),
            },
          ] as const;
        });
        if (!enqueued) {
          return yield* new SessionPersistenceError({
            operation: "enqueue",
            message: `Could not enqueue pending input ${input.id}`,
            cause: new Error("Duplicate input or missing session"),
          });
        }
      });

      const peekPending = Effect.fn("SessionStore.memory.peekPending")(function* (
        sessionId: SessionId,
        kind: PendingInputKind,
      ) {
        const current = yield* Ref.get(state);
        const pending = current.pending.get(sessionId) ?? [];
        const matching = pending
          .filter((input) => input.kind === kind)
          .toSorted(
            (left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id),
          );
        return Option.fromNullishOr(matching[0]);
      });

      const removePending = Effect.fn("SessionStore.memory.removePending")(function* (
        inputId: PendingInputId,
      ) {
        yield* Ref.update(state, (current) => ({
          ...current,
          pending: new Map(
            [...current.pending].map(([sessionId, inputs]) => [
              sessionId,
              inputs.filter((input) => input.id !== inputId),
            ]),
          ),
        }));
      });

      const beginTurn = Effect.fn("SessionStore.memory.beginTurn")(function* (
        inputId: PendingInputId,
        turn: StoredTurn,
        firstMessage: StoredMessage,
      ) {
        const committed = yield* Ref.modify(state, (current) => {
          const pending = current.pending.get(firstMessage.sessionId) ?? [];
          const input = pending.find((input) => input.id === inputId);
          const hasActiveTurn = [...current.turns.values()].some(
            (existing) => existing.sessionId === turn.sessionId && existing.status === "active",
          );
          if (
            input === undefined ||
            !Equal.equals(input.message, firstMessage.message) ||
            turn.status !== "active" ||
            turn.sessionId !== firstMessage.sessionId ||
            firstMessage.turnId !== turn.id ||
            current.turns.has(turn.id) ||
            current.messages.has(firstMessage.id) ||
            hasActiveTurn
          ) {
            return [false, current] as const;
          }
          return [
            true,
            {
              ...current,
              turns: new Map(current.turns).set(turn.id, turn),
              messages: new Map(current.messages).set(firstMessage.id, firstMessage),
              context: new Map(current.context).set(firstMessage.sessionId, [
                ...(current.context.get(firstMessage.sessionId) ?? []),
                firstMessage.id,
              ]),
              pending: new Map(current.pending).set(
                firstMessage.sessionId,
                pending.filter((input) => input.id !== inputId),
              ),
            },
          ] as const;
        });
        if (!committed) {
          return yield* new SessionPersistenceError({
            operation: "beginTurn",
            message: `Could not atomically start turn ${turn.id} from pending input ${inputId}`,
            cause: new Error("Pending input missing or turn/message conflict"),
          });
        }
      });

      const appendPendingMessage = Effect.fn("SessionStore.memory.appendPendingMessage")(function* (
        inputId: PendingInputId,
        message: StoredMessage,
      ) {
        const committed = yield* Ref.modify(state, (current) => {
          const pending = current.pending.get(message.sessionId) ?? [];
          const input = pending.find((input) => input.id === inputId);
          if (
            input === undefined ||
            !Equal.equals(input.message, message.message) ||
            current.messages.has(message.id)
          ) {
            return [false, current] as const;
          }
          return [
            true,
            {
              ...current,
              messages: new Map(current.messages).set(message.id, message),
              context: new Map(current.context).set(message.sessionId, [
                ...(current.context.get(message.sessionId) ?? []),
                message.id,
              ]),
              pending: new Map(current.pending).set(
                message.sessionId,
                pending.filter((input) => input.id !== inputId),
              ),
            },
          ] as const;
        });
        if (!committed) {
          return yield* new SessionPersistenceError({
            operation: "appendPendingMessage",
            message: `Could not atomically append pending input ${inputId}`,
            cause: new Error("Pending input missing or message conflict"),
          });
        }
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
        getTurn,
        activeTurn,
        updateTurnStatus,
        appendMessage,
        nextMessageSequence,
        activeMessages,
        replaceActiveContext,
        commitCompaction,
        enqueue,
        peekPending,
        removePending,
        beginTurn,
        appendPendingMessage,
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
