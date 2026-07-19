import {
  MessageId,
  PendingInputId,
  SessionId,
  TimestampMillis,
  TokenCount,
  TurnId,
} from "@compass/contracts";
import { Clock, Crypto, Effect, type PlatformError } from "effect";

const uuid = Effect.fn("HarnessIds.uuid")(function* (): Effect.fn.Return<
  string,
  PlatformError.PlatformError,
  Crypto.Crypto
> {
  const crypto = yield* Crypto.Crypto;
  return yield* crypto.randomUUIDv7;
});

export const makeSessionId: Effect.Effect<SessionId, PlatformError.PlatformError, Crypto.Crypto> =
  Effect.map(uuid(), (value) => SessionId.make(value));
export const makeTurnId: Effect.Effect<TurnId, PlatformError.PlatformError, Crypto.Crypto> =
  Effect.map(uuid(), (value) => TurnId.make(value));
export const makeMessageId: Effect.Effect<MessageId, PlatformError.PlatformError, Crypto.Crypto> =
  Effect.map(uuid(), (value) => MessageId.make(value));
export const makePendingInputId: Effect.Effect<
  PendingInputId,
  PlatformError.PlatformError,
  Crypto.Crypto
> = Effect.map(uuid(), (value) => PendingInputId.make(value));

export const now = Effect.map(Clock.currentTimeMillis, (value) => TimestampMillis.make(value));

export const tokenCount = (value: number) => TokenCount.make(value);
