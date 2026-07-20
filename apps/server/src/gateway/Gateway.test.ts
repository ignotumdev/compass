import {
  AdapterName,
  ChannelId,
  type GatewayAdapter,
  GatewayMessageId,
  GatewayTimestampMillis,
  IncomingMessage,
  MessageSender,
  OutgoingMessage,
  SentMessage,
} from "@compass/contracts";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeGateway } from "./Gateway.ts";
import type { GatewaySettings } from "./GatewayConfig.ts";

const settings = (channels: ReadonlyArray<string>): GatewaySettings => ({
  allowedChannels: new Set(channels.map((channel) => ChannelId.make(channel))),
  telegram: {
    pollingTimeoutSeconds: 30,
    pollingLimit: 100,
    retryBaseDelayMillis: 1_000,
    retryMaxDelayMillis: 30_000,
    incomingBufferCapacity: 16,
    maxInboundFileBytes: 50 * 1024 * 1024,
    deleteWebhook: false,
    dropPendingUpdates: false,
  },
});

const incomingMessage = (channel: string, text: string) =>
  new IncomingMessage({
    id: GatewayMessageId.make(text),
    channel: ChannelId.make(channel),
    sender: new MessageSender({
      id: "sender",
      displayName: "Sender",
      isBot: false,
    }),
    text,
    files: [],
    receivedAt: GatewayTimestampMillis.make(1),
  });

describe("Gateway", () => {
  it.effect("filters denied channels before materializing their messages", () =>
    Effect.gen(function* () {
      let deniedMaterializations = 0;
      const adapter: GatewayAdapter = {
        name: AdapterName.make("telegram"),
        incoming: Stream.fromIterable([
          {
            channel: ChannelId.make("telegram:denied"),
            materialize: Effect.sync(() => {
              deniedMaterializations += 1;
              return incomingMessage("telegram:denied", "denied");
            }),
          },
          {
            channel: ChannelId.make("telegram:allowed"),
            materialize: Effect.succeed(incomingMessage("telegram:allowed", "allowed")),
          },
        ]),
        send: (message) =>
          Effect.succeed(
            new SentMessage({
              channel: message.channel,
              messageIds: [GatewayMessageId.make("sent")],
            }),
          ),
      };
      const gateway = yield* makeGateway([adapter], settings(["telegram:allowed"]));
      const received = Array.from(yield* gateway.incoming.pipe(Stream.runCollect));

      expect(received.map((message) => message.text)).toEqual(["allowed"]);
      expect(deniedMaterializations).toBe(0);
    }),
  );

  it.effect("authorizes and routes outgoing messages by adapter-qualified channel", () =>
    Effect.gen(function* () {
      const sent: Array<OutgoingMessage> = [];
      const adapter: GatewayAdapter = {
        name: AdapterName.make("telegram"),
        incoming: Stream.empty,
        send: (message) => {
          sent.push(message);
          return Effect.succeed(
            new SentMessage({
              channel: message.channel,
              messageIds: [GatewayMessageId.make("42")],
            }),
          );
        },
      };
      const gateway = yield* makeGateway([adapter], settings(["telegram:allowed"]));
      const message = new OutgoingMessage({
        channel: ChannelId.make("telegram:allowed"),
        text: "hello",
        files: [],
      });

      const result = yield* gateway.send(message);
      expect(result.messageIds).toEqual(["42"]);
      expect(sent).toEqual([message]);

      const denied = yield* gateway
        .send(
          new OutgoingMessage({
            channel: ChannelId.make("telegram:denied"),
            text: "no",
            files: [],
          }),
        )
        .pipe(Effect.flip);
      expect(denied._tag).toBe("GatewayAuthorizationError");
    }),
  );

  it.effect("consumes an outgoing stream with ordered, bounded delivery", () =>
    Effect.gen(function* () {
      const delivered: Array<string> = [];
      const adapter: GatewayAdapter = {
        name: AdapterName.make("telegram"),
        incoming: Stream.empty,
        send: (message) => {
          delivered.push(message.text ?? "");
          return Effect.succeed(
            new SentMessage({
              channel: message.channel,
              messageIds: [GatewayMessageId.make(message.text ?? "sent")],
            }),
          );
        },
      };
      const gateway = yield* makeGateway([adapter], settings(["telegram:allowed"]));
      const outgoing = Stream.fromIterable(
        ["first", "second"].map(
          (text) =>
            new OutgoingMessage({
              channel: ChannelId.make("telegram:allowed"),
              text,
              files: [],
            }),
        ),
      );

      const receipts = Array.from(yield* gateway.sendAll(outgoing).pipe(Stream.runCollect));
      expect(delivered).toEqual(["first", "second"]);
      expect(receipts.map((receipt) => receipt.messageIds[0])).toEqual(["first", "second"]);
    }),
  );
});
