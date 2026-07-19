import { describe, expect, it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import {
  EventBufferSize,
  DatabaseFilename,
  MessageId,
  MessageSequence,
  ModelKey,
  PendingInputId,
  ProviderKey,
  SessionConfiguration,
  SessionId,
  StoredMessage,
  StoredPendingInput,
  StoredSession,
  StoredTurn,
  TimestampMillis,
  TokenLimit,
  TurnId,
} from "@compass/contracts";
import { Context, Effect, FileSystem, Layer, Option, Path } from "effect";
import { TestClock } from "effect/testing";
import { Prompt } from "effect/unstable/ai";
import { SessionStore } from "./SessionStore.ts";
import * as SqliteSessionStore from "./persistence/SqliteSessionStore.ts";

const sessionId = SessionId.make("0198ee50-2c74-7000-8000-000000000001");
const turnId = TurnId.make("0198ee50-2c74-7000-8000-000000000002");
const createdAt = TimestampMillis.make(1_700_000_000_000);

const session = new StoredSession({
  id: sessionId,
  configuration: new SessionConfiguration({
    provider: ProviderKey.make("test"),
    model: ModelKey.make("scripted"),
    systemInstructions: "Be precise.",
    compactAtTokens: TokenLimit.make(10_000),
    summaryMaxTokens: TokenLimit.make(1_000),
    eventBufferSize: EventBufferSize.make(64),
  }),
  createdAt,
  updatedAt: createdAt,
});

const userMessage = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] });

const storedMessage = (
  id: string,
  sequence: number,
  text: string,
  requestedTurnId: TurnId = turnId,
) =>
  new StoredMessage({
    id: MessageId.make(id),
    sessionId,
    turnId: requestedTurnId,
    sequence: MessageSequence.make(sequence),
    message: userMessage(text),
    status: "complete",
    createdAt,
    updatedAt: createdAt,
  });

const exerciseStore = Effect.gen(function* () {
  const store = yield* SessionStore;
  yield* store.createSession(session);
  yield* store.createTurn(
    new StoredTurn({
      id: turnId,
      sessionId,
      status: "active",
      createdAt,
      updatedAt: createdAt,
    }),
  );
  yield* TestClock.setTime(createdAt + 1_000);
  yield* store.updateTurnStatus(turnId, "completed");
  const updatedTurn = Option.getOrThrow(yield* store.getTurn(turnId));
  expect(updatedTurn.status).toBe("completed");
  expect(updatedTurn.updatedAt).toBeGreaterThan(createdAt);

  const first = storedMessage("0198ee50-2c74-7000-8000-000000000003", 0, "first");
  const latest = storedMessage("0198ee50-2c74-7000-8000-000000000004", 1, "latest");
  const summary = storedMessage("0198ee50-2c74-7000-8000-000000000005", 2, "summary");
  yield* store.appendMessage(first, true);
  const duplicateMessage = yield* Effect.flip(store.appendMessage(first, true));
  expect(duplicateMessage._tag).toBe("SessionPersistenceError");
  yield* store.appendMessage(latest, true);
  yield* store.commitCompaction(summary, latest.id);

  const queue = new StoredPendingInput({
    id: PendingInputId.make("0198ee50-2c74-7000-8000-000000000006"),
    sessionId,
    kind: "queue",
    message: userMessage("queued"),
    createdAt,
  });
  const steer = new StoredPendingInput({
    id: PendingInputId.make("0198ee50-2c74-7000-8000-000000000007"),
    sessionId,
    kind: "steer",
    message: userMessage("steer"),
    createdAt: TimestampMillis.make(createdAt + 1),
  });
  const earlierQueue = new StoredPendingInput({
    id: PendingInputId.make("0198ee50-2c74-7000-8000-000000000008"),
    sessionId,
    kind: "queue",
    message: userMessage("earlier queued"),
    createdAt: TimestampMillis.make(createdAt - 1),
  });
  yield* store.enqueue(queue);
  yield* store.enqueue(steer);
  yield* store.enqueue(earlierQueue);

  const active = yield* store.activeMessages(sessionId);
  const counts = yield* store.pendingCounts(sessionId);
  const peekedQueue = yield* store.peekPending(sessionId, "queue");
  const countsAfterPeek = yield* store.pendingCounts(sessionId);
  yield* store.removePending(Option.getOrThrow(peekedQueue).id);
  const after = yield* store.pendingCounts(sessionId);
  const duplicateSession = yield* Effect.flip(store.createSession(session));
  const activeAfterDuplicate = yield* store.activeMessages(sessionId);

  const nextTurnId = TurnId.make("0198ee50-2c74-7000-8000-000000000009");
  const nextTurn = new StoredTurn({
    id: nextTurnId,
    sessionId,
    status: "active",
    createdAt: TimestampMillis.make(createdAt + 2_000),
    updatedAt: TimestampMillis.make(createdAt + 2_000),
  });
  const queuedMessage = storedMessage(
    "0198ee50-2c74-7000-8000-000000000010",
    3,
    "queued",
    nextTurnId,
  );
  const failedBegin = yield* Effect.flip(store.beginTurn(queue.id, nextTurn, first));
  expect((yield* store.pendingCounts(sessionId)).queue).toBe(1);
  yield* store.beginTurn(queue.id, nextTurn, queuedMessage);
  const steeringMessage = storedMessage(
    "0198ee50-2c74-7000-8000-000000000011",
    4,
    "steer",
    nextTurnId,
  );
  yield* store.appendPendingMessage(steer.id, steeringMessage);
  const activeTurn = Option.getOrThrow(yield* store.activeTurn(sessionId));
  const finalCounts = yield* store.pendingCounts(sessionId);
  const finalActive = yield* store.activeMessages(sessionId);

  expect(active.map((message) => message.id)).toEqual([summary.id, latest.id]);
  expect(activeAfterDuplicate.map((message) => message.id)).toEqual([summary.id, latest.id]);
  expect(counts).toEqual({ queue: 2, steer: 1 });
  expect(countsAfterPeek).toEqual(counts);
  expect(Option.getOrThrow(peekedQueue).id).toBe(earlierQueue.id);
  expect(after).toEqual({ queue: 1, steer: 1 });
  expect(duplicateSession._tag).toBe("SessionPersistenceError");
  expect(failedBegin._tag).toBe("SessionPersistenceError");
  expect(activeTurn.id).toBe(nextTurnId);
  expect(finalCounts).toEqual({ queue: 0, steer: 0 });
  expect(finalActive.map((message) => message.id)).toEqual([
    summary.id,
    latest.id,
    queuedMessage.id,
    steeringMessage.id,
  ]);
});

describe("SessionStore", () => {
  it.effect("keeps compaction and pending inputs consistent in memory", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(SessionStore.layerMemory);
        yield* exerciseStore.pipe(Effect.provideContext(context));
      }),
    ),
  );

  it.effect("persists the same contract after reopening SQLite", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const platform = yield* Layer.build(NodeServices.layer);
        const fileSystem = Context.get(platform, FileSystem.FileSystem);
        const path = Context.get(platform, Path.Path);
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "compass-harness-",
        });
        const filename = path.join(directory, "state.db");

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              SqliteSessionStore.layer({
                filename: DatabaseFilename.make(filename),
              }),
            );
            yield* exerciseStore.pipe(Effect.provideContext(context));
          }),
        );

        const reopenedContext = yield* Layer.build(
          SqliteSessionStore.layer({
            filename: DatabaseFilename.make(filename),
          }),
        );
        const reopened = Context.get(reopenedContext, SessionStore);
        const loaded = yield* reopened.getSession(sessionId);
        expect(Option.getOrThrow(loaded)).toEqual(session);
        const active = yield* reopened.activeMessages(sessionId);
        expect(active.map((message) => message.sequence)).toEqual([2, 1, 3, 4]);

        const foreignKeyFailure = yield* Effect.flip(
          reopened.enqueue(
            new StoredPendingInput({
              id: PendingInputId.make("0198ee50-2c74-7000-8000-000000000098"),
              sessionId: SessionId.make("0198ee50-2c74-7000-8000-000000000099"),
              kind: "queue",
              message: userMessage("must not persist"),
              createdAt,
            }),
          ),
        );
        expect(foreignKeyFailure._tag).toBe("SessionPersistenceError");
      }),
    ),
  );
});
