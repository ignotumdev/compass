import { type Effect, Schema, type Stream } from "effect";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));
const NonNegativeInteger = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));
const QUALIFIED_CHANNEL_PATTERN = /^[a-z][a-z0-9-]*:.+$/;

export const AdapterName = NonEmptyString.pipe(Schema.brand("GatewayAdapterName"));
export type AdapterName = typeof AdapterName.Type;

export const ChannelId = Schema.String.check(Schema.isPattern(QUALIFIED_CHANNEL_PATTERN)).pipe(
  Schema.brand("GatewayChannelId"),
);
export type ChannelId = typeof ChannelId.Type;

export const GatewayMessageId = NonEmptyString.pipe(Schema.brand("GatewayMessageId"));
export type GatewayMessageId = typeof GatewayMessageId.Type;

export const ThreadId = NonEmptyString.pipe(Schema.brand("GatewayThreadId"));
export type ThreadId = typeof ThreadId.Type;

export const GatewayTimestampMillis = NonNegativeInteger.pipe(
  Schema.brand("GatewayTimestampMillis"),
);
export type GatewayTimestampMillis = typeof GatewayTimestampMillis.Type;

export const GatewayFileKind = Schema.Literals(["image", "audio", "video", "file"]);
export type GatewayFileKind = typeof GatewayFileKind.Type;

export class GatewayFile extends Schema.Class<GatewayFile>(
  "@compass/contracts/gateway/GatewayFile",
)({
  path: NonEmptyString,
  name: NonEmptyString,
  kind: GatewayFileKind,
  mediaType: Schema.optionalKey(NonEmptyString),
  size: Schema.optionalKey(NonNegativeInteger),
}) {}

export class MessageSender extends Schema.Class<MessageSender>(
  "@compass/contracts/gateway/MessageSender",
)({
  id: NonEmptyString,
  displayName: NonEmptyString,
  username: Schema.optionalKey(NonEmptyString),
  isBot: Schema.Boolean,
}) {}

export class IncomingMessage extends Schema.Class<IncomingMessage>(
  "@compass/contracts/gateway/IncomingMessage",
)({
  id: GatewayMessageId,
  channel: ChannelId,
  thread: Schema.optionalKey(ThreadId),
  sender: MessageSender,
  text: Schema.optionalKey(Schema.String),
  files: Schema.Array(GatewayFile),
  receivedAt: GatewayTimestampMillis,
}) {}

export class OutgoingMessage extends Schema.Class<OutgoingMessage>(
  "@compass/contracts/gateway/OutgoingMessage",
)({
  channel: ChannelId,
  thread: Schema.optionalKey(ThreadId),
  text: Schema.optionalKey(Schema.String),
  files: Schema.Array(GatewayFile),
}) {}

export class SentMessage extends Schema.Class<SentMessage>(
  "@compass/contracts/gateway/SentMessage",
)({
  channel: ChannelId,
  thread: Schema.optionalKey(ThreadId),
  messageIds: Schema.Array(GatewayMessageId),
}) {}

export class GatewayConfigurationError extends Schema.TaggedErrorClass<GatewayConfigurationError>()(
  "GatewayConfigurationError",
  {
    path: Schema.String,
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class GatewayAuthorizationError extends Schema.TaggedErrorClass<GatewayAuthorizationError>()(
  "GatewayAuthorizationError",
  { channel: ChannelId },
) {}

export class GatewayAdapterNotFoundError extends Schema.TaggedErrorClass<GatewayAdapterNotFoundError>()(
  "GatewayAdapterNotFoundError",
  { adapter: AdapterName, channel: ChannelId },
) {}

export class GatewayInvalidMessageError extends Schema.TaggedErrorClass<GatewayInvalidMessageError>()(
  "GatewayInvalidMessageError",
  { channel: ChannelId, message: Schema.String },
) {}

export class GatewayFileError extends Schema.TaggedErrorClass<GatewayFileError>()(
  "GatewayFileError",
  {
    operation: NonEmptyString,
    path: Schema.String,
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class GatewayTransportError extends Schema.TaggedErrorClass<GatewayTransportError>()(
  "GatewayTransportError",
  {
    adapter: AdapterName,
    operation: NonEmptyString,
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class GatewayProtocolError extends Schema.TaggedErrorClass<GatewayProtocolError>()(
  "GatewayProtocolError",
  {
    adapter: AdapterName,
    operation: NonEmptyString,
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export type GatewayAdapterError = GatewayFileError | GatewayTransportError | GatewayProtocolError;

export type GatewayError =
  | GatewayAdapterError
  | GatewayConfigurationError
  | GatewayAuthorizationError
  | GatewayAdapterNotFoundError
  | GatewayInvalidMessageError;

export interface ParsedChannelId {
  readonly adapter: AdapterName;
  readonly nativeChannel: string;
}

export const parseChannelId = (channel: ChannelId): ParsedChannelId => {
  const separator = channel.indexOf(":");
  return {
    adapter: AdapterName.make(channel.slice(0, separator)),
    nativeChannel: channel.slice(separator + 1),
  };
};

export interface IncomingAdapterEvent {
  readonly channel: ChannelId;
  readonly materialize: Effect.Effect<IncomingMessage, GatewayAdapterError>;
}

export interface GatewayAdapter {
  readonly name: AdapterName;
  readonly incoming: Stream.Stream<IncomingAdapterEvent, GatewayAdapterError>;
  readonly send: (message: OutgoingMessage) => Effect.Effect<SentMessage, GatewayAdapterError>;
}
