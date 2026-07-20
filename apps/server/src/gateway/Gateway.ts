import {
  type GatewayAdapter,
  type GatewayError,
  GatewayAdapterNotFoundError,
  GatewayAuthorizationError,
  GatewayConfigurationError,
  GatewayInvalidMessageError,
  type IncomingMessage,
  type OutgoingMessage,
  type SentMessage,
  parseChannelId,
} from "@compass/contracts";
import { Context, Effect, Layer, Stream } from "effect";
import type { GatewaySettings } from "./GatewayConfig.ts";
import { GatewayConfig } from "./GatewayConfig.ts";

export interface SendAllOptions {
  readonly concurrency?: number | "unbounded" | undefined;
}

export interface GatewayService {
  readonly incoming: Stream.Stream<IncomingMessage, GatewayError>;
  readonly send: (message: OutgoingMessage) => Effect.Effect<SentMessage, GatewayError>;
  readonly sendAll: <E, R>(
    outgoing: Stream.Stream<OutgoingMessage, E, R>,
    options?: SendAllOptions,
  ) => Stream.Stream<SentMessage, GatewayError | E, R>;
}

export class Gateway extends Context.Service<Gateway, GatewayService>()(
  "@compass/server/gateway/Gateway",
) {
  static readonly layer = (adapters: ReadonlyArray<GatewayAdapter>) =>
    Layer.effect(
      Gateway,
      Effect.gen(function* () {
        const settings = yield* GatewayConfig;
        return yield* makeGateway(adapters, settings);
      }),
    );
}

export const makeGateway = Effect.fn("Gateway.make")(function* (
  adapters: ReadonlyArray<GatewayAdapter>,
  settings: GatewaySettings,
) {
  const byName = new Map(adapters.map((adapter) => [adapter.name, adapter] as const));
  if (byName.size !== adapters.length) {
    return yield* new GatewayConfigurationError({
      path: "adapters",
      message: "Gateway adapter names must be unique",
      cause: new Error("Duplicate gateway adapter name"),
    });
  }

  const incoming = Stream.mergeAll(
    adapters.map((adapter) => adapter.incoming),
    { concurrency: "unbounded", bufferSize: 16 },
  ).pipe(
    Stream.filter((event) => settings.allowedChannels.has(event.channel)),
    Stream.mapEffect((event) => event.materialize),
  );

  const send = Effect.fn("Gateway.send")(function* (message: OutgoingMessage) {
    if (!settings.allowedChannels.has(message.channel)) {
      return yield* new GatewayAuthorizationError({ channel: message.channel });
    }
    if ((message.text === undefined || message.text.length === 0) && message.files.length === 0) {
      return yield* new GatewayInvalidMessageError({
        channel: message.channel,
        message: "An outgoing message must contain text or at least one file",
      });
    }
    const parsed = parseChannelId(message.channel);
    const adapter = byName.get(parsed.adapter);
    if (adapter === undefined) {
      return yield* new GatewayAdapterNotFoundError({
        adapter: parsed.adapter,
        channel: message.channel,
      });
    }
    return yield* adapter.send(message);
  });

  const sendAll: GatewayService["sendAll"] = (outgoing, options) =>
    outgoing.pipe(
      Stream.mapEffect(send, {
        concurrency: options?.concurrency ?? 1,
      }),
    );

  return Gateway.of({ incoming, send, sendAll });
});
