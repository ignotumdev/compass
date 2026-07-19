import {
  ContextPosition,
  DatabaseFilename,
  MessageSequence,
  PendingCounts,
  type PendingInputKind,
  SessionConfiguration,
  type SessionId,
  SessionPersistenceError,
  StoredMessage,
  type StoredPendingInput,
  StoredPendingInput as StoredPendingInputSchema,
  type StoredSession,
  StoredSession as StoredSessionSchema,
  type SqliteSessionStoreOptions,
  type StoredTurn,
  TokenCount,
  type TurnId,
  type TurnStatus,
} from "@compass/contracts";
import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-node";
import { Config, Effect, FileSystem, Layer, Option, Path, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";
import { SqlClient } from "effect/unstable/sql";
import { SessionStore, type SessionStoreService } from "../SessionStore.ts";

const SessionRow = Schema.Struct({
  id: Schema.String,
  configuration: Schema.String,
  createdAt: Schema.Finite,
  updatedAt: Schema.Finite,
});

const MessageRow = Schema.Struct({
  id: Schema.String,
  sessionId: Schema.String,
  turnId: Schema.NullOr(Schema.String),
  sequence: Schema.Finite,
  message: Schema.String,
  status: Schema.String,
  createdAt: Schema.Finite,
  updatedAt: Schema.Finite,
});

const PendingRow = Schema.Struct({
  id: Schema.String,
  sessionId: Schema.String,
  kind: Schema.String,
  message: Schema.String,
  createdAt: Schema.Finite,
});

const PositionRow = Schema.Struct({ position: Schema.Finite });
const SequenceRow = Schema.Struct({ sequence: Schema.Finite });
const CountsRow = Schema.Struct({ queue: Schema.Finite, steer: Schema.Finite });

const migration = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* sql`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      configuration TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS turns (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      status TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
      sequence INTEGER NOT NULL,
      message TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE(session_id, sequence)
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS context_messages (
      session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      position INTEGER NOT NULL,
      message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      PRIMARY KEY(session_id, position),
      UNIQUE(session_id, message_id)
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS pending_inputs (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS pending_inputs_fifo
    ON pending_inputs(session_id, kind, created_at, id)
  `;
});

const migrations = SqliteMigrator.fromRecord({ "1_harness": migration });

const mapPersistenceError =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.mapError(
      effect,
      (cause) =>
        new SessionPersistenceError({
          operation,
          message: `Session persistence operation failed: ${operation}`,
          cause,
        }),
    );

const parseJson = (text: string) =>
  Effect.try({
    try: () => JSON.parse(text) as unknown,
    catch: (cause) =>
      new SessionPersistenceError({
        operation: "decode JSON",
        message: "Could not decode persisted session JSON",
        cause,
      }),
  });

const encodeJson = <A, I, R>(schema: Schema.Codec<A, I, R>, value: A) =>
  Schema.encodeEffect(schema)(value).pipe(Effect.map((encoded) => JSON.stringify(encoded)));

const decodeSession = Effect.fn("SqliteSessionStore.decodeSession")(function* (input: unknown) {
  const row = yield* Schema.decodeUnknownEffect(SessionRow)(input);
  const configurationJson = yield* parseJson(row.configuration);
  const configuration = yield* Schema.decodeUnknownEffect(SessionConfiguration)(configurationJson);
  return yield* Schema.decodeUnknownEffect(StoredSessionSchema)({
    id: row.id,
    configuration,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
});

const decodeMessage = Effect.fn("SqliteSessionStore.decodeMessage")(function* (input: unknown) {
  const row = yield* Schema.decodeUnknownEffect(MessageRow)(input);
  const messageJson = yield* parseJson(row.message);
  const message = yield* Schema.decodeUnknownEffect(Prompt.Message)(messageJson);
  return yield* Schema.decodeUnknownEffect(StoredMessage)({
    id: row.id,
    sessionId: row.sessionId,
    turnId: row.turnId,
    sequence: row.sequence,
    message,
    status: row.status,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
});

const decodePending = Effect.fn("SqliteSessionStore.decodePending")(function* (input: unknown) {
  const row = yield* Schema.decodeUnknownEffect(PendingRow)(input);
  const messageJson = yield* parseJson(row.message);
  const message = yield* Schema.decodeUnknownEffect(Prompt.UserMessage)(messageJson);
  return yield* Schema.decodeUnknownEffect(StoredPendingInputSchema)({
    id: row.id,
    sessionId: row.sessionId,
    kind: row.kind,
    message,
    createdAt: row.createdAt,
  });
});

const make = Effect.fn("SqliteSessionStore.make")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* SqliteMigrator.run({ loader: migrations });

  const createSession = Effect.fn("SqliteSessionStore.createSession")(function* (
    session: StoredSession,
  ) {
    const configuration = yield* encodeJson(SessionConfiguration, session.configuration);
    yield* sql`
        INSERT INTO sessions (id, configuration, created_at, updated_at)
        VALUES (${session.id}, ${configuration}, ${session.createdAt}, ${session.updatedAt})
      `;
  }, mapPersistenceError("createSession"));

  const getSession = Effect.fn("SqliteSessionStore.getSession")(function* (sessionId: SessionId) {
    const rows = yield* sql`
        SELECT id, configuration, created_at AS createdAt, updated_at AS updatedAt
        FROM sessions
        WHERE id = ${sessionId}
        LIMIT 1
      `;
    if (rows.length === 0) return Option.none<StoredSession>();
    return Option.some(yield* decodeSession(rows[0]));
  }, mapPersistenceError("getSession"));

  const createTurn = Effect.fn("SqliteSessionStore.createTurn")(function* (turn: StoredTurn) {
    yield* sql`
        INSERT INTO turns (id, session_id, status, created_at, updated_at)
        VALUES (${turn.id}, ${turn.sessionId}, ${turn.status}, ${turn.createdAt}, ${turn.updatedAt})
      `;
  }, mapPersistenceError("createTurn"));

  const updateTurnStatus = Effect.fn("SqliteSessionStore.updateTurnStatus")(function* (
    turnId: TurnId,
    status: TurnStatus,
  ) {
    yield* sql`
        UPDATE turns
        SET status = ${status}, updated_at = unixepoch('subsec') * 1000
        WHERE id = ${turnId}
      `;
  }, mapPersistenceError("updateTurnStatus"));

  const appendMessage = Effect.fn("SqliteSessionStore.appendMessage")(function* (
    message: StoredMessage,
    active: boolean,
  ) {
    const encoded = yield* encodeJson(Prompt.Message, message.message);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
            INSERT INTO messages (
              id, session_id, turn_id, sequence, message, status, created_at, updated_at
            ) VALUES (
              ${message.id}, ${message.sessionId}, ${message.turnId}, ${message.sequence},
              ${encoded}, ${message.status}, ${message.createdAt}, ${message.updatedAt}
            )
          `;
        if (active) {
          const rows = yield* sql`
              SELECT COALESCE(MAX(position), -1) + 1 AS position
              FROM context_messages
              WHERE session_id = ${message.sessionId}
            `;
          const row = yield* Schema.decodeUnknownEffect(PositionRow)(rows[0]);
          yield* sql`
              INSERT INTO context_messages (session_id, position, message_id)
              VALUES (${message.sessionId}, ${ContextPosition.make(row.position)}, ${message.id})
            `;
        }
      }),
    );
  }, mapPersistenceError("appendMessage"));

  const nextMessageSequence = Effect.fn("SqliteSessionStore.nextMessageSequence")(function* (
    sessionId: SessionId,
  ) {
    const rows = yield* sql`
        SELECT COALESCE(MAX(sequence), -1) + 1 AS sequence
        FROM messages
        WHERE session_id = ${sessionId}
      `;
    const row = yield* Schema.decodeUnknownEffect(SequenceRow)(rows[0]);
    return MessageSequence.make(row.sequence);
  }, mapPersistenceError("nextMessageSequence"));

  const activeMessages = Effect.fn("SqliteSessionStore.activeMessages")(function* (
    sessionId: SessionId,
  ) {
    const rows = yield* sql`
        SELECT
          messages.id,
          messages.session_id AS sessionId,
          messages.turn_id AS turnId,
          messages.sequence,
          messages.message,
          messages.status,
          messages.created_at AS createdAt,
          messages.updated_at AS updatedAt
        FROM context_messages
        INNER JOIN messages ON messages.id = context_messages.message_id
        WHERE context_messages.session_id = ${sessionId}
        ORDER BY context_messages.position ASC
      `;
    return yield* Effect.forEach(rows, decodeMessage);
  }, mapPersistenceError("activeMessages"));

  const replaceActiveContext = Effect.fn("SqliteSessionStore.replaceActiveContext")(function* (
    sessionId: SessionId,
    messageIds: ReadonlyArray<StoredMessage["id"]>,
  ) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`DELETE FROM context_messages WHERE session_id = ${sessionId}`;
        yield* Effect.forEach(
          messageIds,
          (messageId, position) =>
            sql`
                INSERT INTO context_messages (session_id, position, message_id)
                VALUES (${sessionId}, ${ContextPosition.make(position)}, ${messageId})
              `,
          { discard: true },
        );
      }),
    );
  }, mapPersistenceError("replaceActiveContext"));

  const commitCompaction = Effect.fn("SqliteSessionStore.commitCompaction")(function* (
    summary: StoredMessage,
    latestUserMessageId: StoredMessage["id"],
  ) {
    const encoded = yield* encodeJson(Prompt.Message, summary.message);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
            INSERT INTO messages (
              id, session_id, turn_id, sequence, message, status, created_at, updated_at
            ) VALUES (
              ${summary.id}, ${summary.sessionId}, ${summary.turnId}, ${summary.sequence},
              ${encoded}, ${summary.status}, ${summary.createdAt}, ${summary.updatedAt}
            )
          `;
        yield* sql`DELETE FROM context_messages WHERE session_id = ${summary.sessionId}`;
        yield* sql`
            INSERT INTO context_messages (session_id, position, message_id)
            VALUES (${summary.sessionId}, ${ContextPosition.make(0)}, ${summary.id})
          `;
        yield* sql`
            INSERT INTO context_messages (session_id, position, message_id)
            VALUES (${summary.sessionId}, ${ContextPosition.make(1)}, ${latestUserMessageId})
          `;
      }),
    );
  }, mapPersistenceError("commitCompaction"));

  const enqueue = Effect.fn("SqliteSessionStore.enqueue")(function* (input: StoredPendingInput) {
    const message = yield* encodeJson(Prompt.UserMessage, input.message);
    yield* sql`
        INSERT INTO pending_inputs (id, session_id, kind, message, created_at)
        VALUES (${input.id}, ${input.sessionId}, ${input.kind}, ${message}, ${input.createdAt})
      `;
  }, mapPersistenceError("enqueue"));

  const takePending = Effect.fn("SqliteSessionStore.takePending")(function* (
    sessionId: SessionId,
    kind: PendingInputKind,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const rows = yield* sql`
            SELECT id, session_id AS sessionId, kind, message, created_at AS createdAt
            FROM pending_inputs
            WHERE session_id = ${sessionId} AND kind = ${kind}
            ORDER BY created_at ASC, id ASC
            LIMIT 1
          `;
        if (rows.length === 0) return Option.none<StoredPendingInput>();
        const pending = yield* decodePending(rows[0]);
        yield* sql`DELETE FROM pending_inputs WHERE id = ${pending.id}`;
        return Option.some(pending);
      }),
    );
  }, mapPersistenceError("takePending"));

  const pendingCounts = Effect.fn("SqliteSessionStore.pendingCounts")(function* (
    sessionId: SessionId,
  ): Effect.fn.Return<PendingCounts, unknown> {
    const rows = yield* sql`
        SELECT
          COALESCE(SUM(CASE WHEN kind = 'queue' THEN 1 ELSE 0 END), 0) AS queue,
          COALESCE(SUM(CASE WHEN kind = 'steer' THEN 1 ELSE 0 END), 0) AS steer
        FROM pending_inputs
        WHERE session_id = ${sessionId}
      `;
    const row = yield* Schema.decodeUnknownEffect(CountsRow)(rows[0]);
    return new PendingCounts({
      queue: TokenCount.make(row.queue),
      steer: TokenCount.make(row.steer),
    });
  }, mapPersistenceError("pendingCounts"));

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
  } satisfies SessionStoreService);
});

export const layer = (options: SqliteSessionStoreOptions) =>
  Layer.effect(SessionStore, make()).pipe(
    Layer.provide(SqliteClient.layer({ filename: options.filename })),
  );

export const layerDefault = Layer.unwrap(
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = yield* Config.string("HOME").pipe(
      Config.orElse(() => Config.string("USERPROFILE")),
    );
    const directory = path.join(home, ".compass");
    yield* fileSystem.makeDirectory(directory, { recursive: true });
    return layer({
      filename: DatabaseFilename.make(path.join(directory, "state.db")),
    });
  }),
);
