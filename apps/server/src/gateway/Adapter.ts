import type { Effect, Stream } from "effect";
import type {
  AdapterName,
  ChannelId,
  GatewayAdapterError,
  IncomingMessage,
  OutgoingMessage,
  SentMessage,
} from "./Models.ts";

export interface IncomingAdapterEvent {
  readonly channel: ChannelId;
  readonly materialize: Effect.Effect<IncomingMessage, GatewayAdapterError>;
}

export interface GatewayAdapter {
  readonly name: AdapterName;
  readonly incoming: Stream.Stream<IncomingAdapterEvent, GatewayAdapterError>;
  readonly send: (message: OutgoingMessage) => Effect.Effect<SentMessage, GatewayAdapterError>;
}
