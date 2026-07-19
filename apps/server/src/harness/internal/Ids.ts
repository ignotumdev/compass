import {
  MessageId,
  PendingInputId,
  SessionId,
  TimestampMillis,
  TokenCount,
  TurnId,
} from "@compass/contracts";
import { Clock, type Crypto, Effect, type PlatformError } from "effect";

const uuid = (crypto: Crypto.Crypto) => crypto.randomUUIDv7;

export const makeSessionId = (
  crypto: Crypto.Crypto,
): Effect.Effect<SessionId, PlatformError.PlatformError> =>
  Effect.map(uuid(crypto), (value) => SessionId.make(value));
export const makeTurnId = (
  crypto: Crypto.Crypto,
): Effect.Effect<TurnId, PlatformError.PlatformError> =>
  Effect.map(uuid(crypto), (value) => TurnId.make(value));
export const makeMessageId = (
  crypto: Crypto.Crypto,
): Effect.Effect<MessageId, PlatformError.PlatformError> =>
  Effect.map(uuid(crypto), (value) => MessageId.make(value));
export const makePendingInputId = (
  crypto: Crypto.Crypto,
): Effect.Effect<PendingInputId, PlatformError.PlatformError> =>
  Effect.map(uuid(crypto), (value) => PendingInputId.make(value));

export const now = Effect.map(Clock.currentTimeMillis, (value) => TimestampMillis.make(value));

export const tokenCount = (value: number) => TokenCount.make(value);
