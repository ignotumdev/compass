import { type Effect, type Layer, Schema } from "effect";
import { LanguageModel, Prompt, Response, Tool, Toolkit } from "effect/unstable/ai";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));
const NonNegativeInteger = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));
const PositiveInteger = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThan(0));

export const SessionId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("SessionId"));
export type SessionId = typeof SessionId.Type;

export const TurnId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("TurnId"));
export type TurnId = typeof TurnId.Type;

export const MessageId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("MessageId"));
export type MessageId = typeof MessageId.Type;

export const PendingInputId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("PendingInputId"),
);
export type PendingInputId = typeof PendingInputId.Type;

export const MessageSequence = NonNegativeInteger.pipe(Schema.brand("MessageSequence"));
export type MessageSequence = typeof MessageSequence.Type;

export const ContextPosition = NonNegativeInteger.pipe(Schema.brand("ContextPosition"));
export type ContextPosition = typeof ContextPosition.Type;

export const TokenCount = NonNegativeInteger.pipe(Schema.brand("TokenCount"));
export type TokenCount = typeof TokenCount.Type;

export const TokenLimit = PositiveInteger.pipe(Schema.brand("TokenLimit"));
export type TokenLimit = typeof TokenLimit.Type;

export const EventBufferSize = PositiveInteger.pipe(Schema.brand("EventBufferSize"));
export type EventBufferSize = typeof EventBufferSize.Type;

export const TimestampMillis = NonNegativeInteger.pipe(Schema.brand("TimestampMillis"));
export type TimestampMillis = typeof TimestampMillis.Type;

export const ProviderKey = NonEmptyString.pipe(Schema.brand("ProviderKey"));
export type ProviderKey = typeof ProviderKey.Type;

export const ModelKey = NonEmptyString.pipe(Schema.brand("ModelKey"));
export type ModelKey = typeof ModelKey.Type;

export const DatabaseFilename = NonEmptyString.pipe(Schema.brand("DatabaseFilename"));
export type DatabaseFilename = typeof DatabaseFilename.Type;

export const SessionPhase = Schema.Literals([
  "idle",
  "checking-tokens",
  "compacting",
  "generating",
  "resolving-tools",
  "settling",
  "failed",
]);
export type SessionPhase = typeof SessionPhase.Type;

export const TurnStatus = Schema.Literals(["active", "completed", "interrupted", "failed"]);
export type TurnStatus = typeof TurnStatus.Type;

export const StoredMessageStatus = Schema.Literals(["streaming", "complete", "interrupted"]);
export type StoredMessageStatus = typeof StoredMessageStatus.Type;

export const PendingInputKind = Schema.Literals(["queue", "steer"]);
export type PendingInputKind = typeof PendingInputKind.Type;

export class SessionConfiguration extends Schema.Class<SessionConfiguration>(
  "@compass/contracts/harness/SessionConfiguration",
)({
  provider: ProviderKey,
  model: ModelKey,
  systemInstructions: Schema.String,
  compactAtTokens: TokenLimit,
  summaryMaxTokens: TokenLimit,
  eventBufferSize: EventBufferSize,
}) {}

export class CreateSessionOptions extends Schema.Class<CreateSessionOptions>(
  "@compass/contracts/harness/CreateSessionOptions",
)({
  id: Schema.optionalKey(SessionId),
  provider: Schema.optionalKey(ProviderKey),
  model: Schema.optionalKey(ModelKey),
  systemInstructions: Schema.optionalKey(Schema.String),
  compactAtTokens: Schema.optionalKey(TokenLimit),
  summaryMaxTokens: Schema.optionalKey(TokenLimit),
  eventBufferSize: Schema.optionalKey(EventBufferSize),
}) {}

export class PendingCounts extends Schema.Class<PendingCounts>(
  "@compass/contracts/harness/PendingCounts",
)({ queue: TokenCount, steer: TokenCount }) {}

export class SqliteSessionStoreOptions extends Schema.Class<SqliteSessionStoreOptions>(
  "@compass/contracts/harness/SqliteSessionStoreOptions",
)({ filename: DatabaseFilename }) {}

export class ModelGenerationOptions extends Schema.Class<ModelGenerationOptions>(
  "@compass/contracts/harness/ModelGenerationOptions",
)({ maxOutputTokens: TokenLimit }) {}

export interface HarnessModelBinding {
  readonly provider: ProviderKey;
  readonly model: ModelKey;
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly transformGeneration?: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    options: ModelGenerationOptions,
  ) => Effect.Effect<A, E, R>;
}

export const ExecuteTool = Tool.make("execute", {
  description: "Execute a command in the Compass environment.",
  parameters: Schema.Struct({
    command: Schema.String.annotate({
      description: "The command to execute.",
    }),
  }),
  success: Schema.String,
});

export const CompactTool = Tool.make("compact", {
  description:
    "Compact the conversation history when the context is too large or a fresh summary would help.",
  parameters: Schema.Struct({}),
  success: Schema.String,
});

export const HarnessToolkit = Toolkit.make(ExecuteTool, CompactTool);

export const HarnessResponsePart = Response.AllParts(HarnessToolkit);
export type HarnessResponsePart = typeof HarnessResponsePart.Type;

export class PromptInput extends Schema.TaggedClass<PromptInput>()("Prompt", {
  message: Prompt.UserMessage,
}) {}

export class QueueInput extends Schema.TaggedClass<QueueInput>()("Queue", {
  message: Prompt.UserMessage,
}) {}

export class SteerInput extends Schema.TaggedClass<SteerInput>()("Steer", {
  message: Prompt.UserMessage,
}) {}

export class StopInput extends Schema.TaggedClass<StopInput>()("Stop", {}) {}

export const SessionInput = Schema.Union([PromptInput, QueueInput, SteerInput, StopInput]);
export type SessionInput = typeof SessionInput.Type;

export class StoredSession extends Schema.Class<StoredSession>(
  "@compass/contracts/harness/StoredSession",
)({
  id: SessionId,
  configuration: SessionConfiguration,
  createdAt: TimestampMillis,
  updatedAt: TimestampMillis,
}) {}

export class StoredTurn extends Schema.Class<StoredTurn>("@compass/contracts/harness/StoredTurn")({
  id: TurnId,
  sessionId: SessionId,
  status: TurnStatus,
  createdAt: TimestampMillis,
  updatedAt: TimestampMillis,
}) {}

export class StoredMessage extends Schema.Class<StoredMessage>(
  "@compass/contracts/harness/StoredMessage",
)({
  id: MessageId,
  sessionId: SessionId,
  turnId: Schema.NullOr(TurnId),
  sequence: MessageSequence,
  message: Prompt.Message,
  status: StoredMessageStatus,
  createdAt: TimestampMillis,
  updatedAt: TimestampMillis,
}) {}

export class StoredPendingInput extends Schema.Class<StoredPendingInput>(
  "@compass/contracts/harness/StoredPendingInput",
)({
  id: PendingInputId,
  sessionId: SessionId,
  kind: PendingInputKind,
  message: Prompt.UserMessage,
  createdAt: TimestampMillis,
}) {}

export class SessionOpened extends Schema.TaggedClass<SessionOpened>()("SessionOpened", {
  sessionId: SessionId,
}) {}

export class InputAccepted extends Schema.TaggedClass<InputAccepted>()("InputAccepted", {
  sessionId: SessionId,
  inputId: Schema.NullOr(PendingInputId),
  kind: Schema.Literals(["prompt", "queue", "steer", "stop"]),
}) {}

export class PhaseChanged extends Schema.TaggedClass<PhaseChanged>()("PhaseChanged", {
  sessionId: SessionId,
  phase: SessionPhase,
}) {}

export class TurnStarted extends Schema.TaggedClass<TurnStarted>()("TurnStarted", {
  sessionId: SessionId,
  turnId: TurnId,
}) {}

export class MessageStarted extends Schema.TaggedClass<MessageStarted>()("MessageStarted", {
  sessionId: SessionId,
  turnId: Schema.NullOr(TurnId),
  messageId: MessageId,
  role: Schema.Literals(["system", "user", "assistant", "tool"]),
}) {}

export class ResponsePartEvent extends Schema.TaggedClass<ResponsePartEvent>()("ResponsePart", {
  sessionId: SessionId,
  turnId: TurnId,
  messageId: MessageId,
  part: HarnessResponsePart,
}) {}

export class MessageCommitted extends Schema.TaggedClass<MessageCommitted>()("MessageCommitted", {
  sessionId: SessionId,
  turnId: Schema.NullOr(TurnId),
  messageId: MessageId,
  role: Schema.Literals(["system", "user", "assistant", "tool"]),
}) {}

export class QueueChanged extends Schema.TaggedClass<QueueChanged>()("QueueChanged", {
  sessionId: SessionId,
  queued: TokenCount,
  steering: TokenCount,
}) {}

export class CompactionStarted extends Schema.TaggedClass<CompactionStarted>()(
  "CompactionStarted",
  { sessionId: SessionId, tokensBefore: TokenCount },
) {}

export class CompactionCompleted extends Schema.TaggedClass<CompactionCompleted>()(
  "CompactionCompleted",
  {
    sessionId: SessionId,
    summaryMessageId: MessageId,
    tokensBefore: TokenCount,
  },
) {}

export class TurnCompleted extends Schema.TaggedClass<TurnCompleted>()("TurnCompleted", {
  sessionId: SessionId,
  turnId: TurnId,
}) {}

export class SessionFailed extends Schema.TaggedClass<SessionFailed>()("SessionFailed", {
  sessionId: SessionId,
  message: Schema.String,
}) {}

export class SessionSettled extends Schema.TaggedClass<SessionSettled>()("SessionSettled", {
  sessionId: SessionId,
}) {}

export const SessionEvent = Schema.Union([
  SessionOpened,
  InputAccepted,
  PhaseChanged,
  TurnStarted,
  MessageStarted,
  ResponsePartEvent,
  MessageCommitted,
  QueueChanged,
  CompactionStarted,
  CompactionCompleted,
  TurnCompleted,
  SessionFailed,
  SessionSettled,
]);
export type SessionEvent = typeof SessionEvent.Type;

export class SessionNotFoundError extends Schema.TaggedErrorClass<SessionNotFoundError>()(
  "SessionNotFoundError",
  { sessionId: SessionId },
) {}

export class SessionBusyError extends Schema.TaggedErrorClass<SessionBusyError>()(
  "SessionBusyError",
  { sessionId: SessionId },
) {}

export class InvalidSessionStateError extends Schema.TaggedErrorClass<InvalidSessionStateError>()(
  "InvalidSessionStateError",
  { sessionId: SessionId, phase: SessionPhase, message: Schema.String },
) {}

export class SessionPersistenceError extends Schema.TaggedErrorClass<SessionPersistenceError>()(
  "SessionPersistenceError",
  { operation: NonEmptyString, message: Schema.String, cause: Schema.Defect() },
) {}

export class SessionConfigurationError extends Schema.TaggedErrorClass<SessionConfigurationError>()(
  "SessionConfigurationError",
  {
    provider: ProviderKey,
    model: ModelKey,
    message: Schema.String,
  },
) {}

export class SessionCompactionError extends Schema.TaggedErrorClass<SessionCompactionError>()(
  "SessionCompactionError",
  { sessionId: SessionId, message: Schema.String, cause: Schema.Defect() },
) {}

export class SessionModelError extends Schema.TaggedErrorClass<SessionModelError>()(
  "SessionModelError",
  { sessionId: SessionId, message: Schema.String, cause: Schema.Defect() },
) {}

export type HarnessError =
  | SessionNotFoundError
  | SessionBusyError
  | InvalidSessionStateError
  | SessionConfigurationError
  | SessionPersistenceError
  | SessionCompactionError
  | SessionModelError;
