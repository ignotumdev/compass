import {
  ChannelId,
  GatewayFile,
  type GatewayAdapterError,
  GatewayFileError,
  GatewayMessageId,
  OutgoingMessage,
} from "@compass/contracts";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Latch, Layer, Stream } from "effect";
import { makeGateway } from "../../Gateway.ts";
import { GatewayConfig, type GatewaySettings } from "../../GatewayConfig.ts";
import { GatewayFiles, type GatewayFilesService } from "../../GatewayFiles.ts";
import { TelegramAdapter } from "./TelegramAdapter.ts";
import { TelegramApi, type TelegramApiService } from "./TelegramApi.ts";

const settings = (allowedChannels: ReadonlyArray<string>): GatewaySettings => ({
  allowedChannels: new Set(allowedChannels.map((channel) => ChannelId.make(channel))),
  telegram: {
    pollingTimeoutSeconds: 30,
    pollingLimit: 100,
    retryBaseDelayMillis: 1,
    retryMaxDelayMillis: 10,
    incomingBufferCapacity: 16,
    maxInboundFileBytes: 50 * 1024 * 1024,
    deleteWebhook: true,
    dropPendingUpdates: false,
  },
});

const baseApi = (): TelegramApiService => ({
  getMe: Effect.succeed({ id: 99, is_bot: true, first_name: "Compass", username: "compass_bot" }),
  deleteWebhook: () => Effect.void,
  getUpdates: () => Effect.never,
  getFile: () => Effect.die("getFile was not expected"),
  download: () => Stream.die("download was not expected"),
  sendText: () => Effect.die("sendText was not expected"),
  sendMedia: () => Effect.die("sendMedia was not expected"),
});

const baseFiles = (): GatewayFilesService => ({
  directory: "C:/gateway-files",
  save: () => Effect.die("save was not expected"),
  saveStream: () => Effect.die("saveStream was not expected"),
  read: () => Effect.die("read was not expected"),
});

const adapterLayer = (
  config: GatewaySettings,
  api: TelegramApiService,
  files: GatewayFilesService,
): Layer.Layer<TelegramAdapter, GatewayAdapterError> =>
  TelegramAdapter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        GatewayConfig.layer(config),
        Layer.succeed(TelegramApi, api),
        GatewayFiles.layer(files),
      ),
    ),
  );

describe("TelegramAdapter", () => {
  it.effect("polls with offsets and lets the gateway reject media before download", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const secondPoll = yield* Latch.make();
        const pollingRequests: Array<{ readonly offset?: number | undefined }> = [];
        let deleteWebhookCalls = 0;
        const api: TelegramApiService = {
          ...baseApi(),
          deleteWebhook: () => {
            deleteWebhookCalls += 1;
            return Effect.void;
          },
          getUpdates: (request) => {
            pollingRequests.push(request);
            if (pollingRequests.length === 1) {
              return Effect.succeed([
                {
                  update_id: 10,
                  message: {
                    message_id: 100,
                    date: 1,
                    chat: { id: 1, type: "private" },
                    from: { id: 1, is_bot: false, first_name: "Denied" },
                    photo: [{ file_id: "secret-photo", width: 10, height: 10 }],
                  },
                },
                {
                  update_id: 11,
                  message: {
                    message_id: 101,
                    date: 2,
                    chat: { id: 2, type: "private" },
                    from: { id: 2, is_bot: false, first_name: "Allowed" },
                    text: "hello",
                  },
                },
              ]);
            }
            return secondPoll.open.pipe(Effect.andThen(Effect.never));
          },
        };
        const config = settings(["telegram:2"]);
        const context = yield* Layer.build(adapterLayer(config, api, baseFiles()));
        const adapter = Context.get(context, TelegramAdapter);
        const gateway = yield* makeGateway([adapter], config);

        const received = Array.from(
          yield* gateway.incoming.pipe(Stream.take(1), Stream.runCollect),
        );
        yield* secondPoll.await;

        expect(received.map((message) => message.text)).toEqual(["hello"]);
        expect(pollingRequests[1]?.offset).toBe(12);
        expect(deleteWebhookCalls).toBe(1);
      }),
    ),
  );

  it.effect("uploads finished image and voice attachments from local paths", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const mediaRequests: Array<{
          readonly method: string;
          readonly caption?: string | undefined;
          readonly filename: string;
        }> = [];
        let nextMessageId = 40;
        const api: TelegramApiService = {
          ...baseApi(),
          sendMedia: (request) => {
            mediaRequests.push(request);
            nextMessageId += 1;
            return Effect.succeed({
              message_id: nextMessageId,
              date: 1,
              chat: { id: 2, type: "private" },
            });
          },
        };
        const files: GatewayFilesService = {
          ...baseFiles(),
          read: () => Effect.succeed(Uint8Array.of(1, 2, 3)),
        };
        const config = {
          ...settings(["telegram:2"]),
          telegram: { ...settings([]).telegram, deleteWebhook: false },
        };
        const context = yield* Layer.build(adapterLayer(config, api, files));
        const adapter = Context.get(context, TelegramAdapter);
        const result = yield* adapter.send(
          new OutgoingMessage({
            channel: ChannelId.make("telegram:2"),
            text: "caption",
            files: [
              new GatewayFile({
                path: "C:/tmp/image.png",
                name: "image.png",
                kind: "image",
                mediaType: "image/png",
              }),
              new GatewayFile({
                path: "C:/tmp/voice.ogg",
                name: "voice.ogg",
                kind: "audio",
                mediaType: "audio/ogg",
              }),
            ],
          }),
        );

        expect(
          mediaRequests.map(({ method, caption, filename }) => ({ method, caption, filename })),
        ).toEqual([
          { method: "sendPhoto", caption: "caption", filename: "image.png" },
          { method: "sendVoice", caption: undefined, filename: "voice.ogg" },
        ]);
        expect(result.messageIds).toEqual([
          GatewayMessageId.make("41"),
          GatewayMessageId.make("42"),
        ]);
      }),
    ),
  );

  it.effect("downloads and materializes supported files for allowed incoming channels", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let polled = false;
        const savedKinds: Array<string> = [];
        const api: TelegramApiService = {
          ...baseApi(),
          getUpdates: () => {
            if (polled) return Effect.never;
            polled = true;
            return Effect.succeed([
              {
                update_id: 20,
                message: {
                  message_id: 200,
                  date: 3,
                  chat: { id: 2, type: "private" },
                  from: { id: 2, is_bot: false, first_name: "Allowed" },
                  caption: "two files",
                  photo: [{ file_id: "photo", width: 20, height: 20 }],
                  voice: { file_id: "voice", mime_type: "audio/ogg" },
                },
              },
            ]);
          },
          getFile: (fileId) =>
            Effect.succeed({ file_id: fileId, file_path: `${fileId}.bin`, file_size: 3 }),
          download: () => Stream.succeed(Uint8Array.of(1, 2, 3)),
        };
        const files: GatewayFilesService = {
          ...baseFiles(),
          saveStream: (options) => {
            savedKinds.push(options.kind);
            return options.stream.pipe(
              Stream.runDrain,
              Effect.mapError(
                (cause) =>
                  new GatewayFileError({
                    operation: "test save",
                    path: "C:/gateway-files",
                    message: "Test attachment stream failed",
                    cause,
                  }),
              ),
              Effect.as(
                new GatewayFile({
                  path: `C:/gateway-files/${options.preferredName}`,
                  name: options.preferredName,
                  kind: options.kind,
                  ...(options.mediaType === undefined ? {} : { mediaType: options.mediaType }),
                  size: 3,
                }),
              ),
            );
          },
        };
        const config = {
          ...settings(["telegram:2"]),
          telegram: { ...settings([]).telegram, deleteWebhook: false },
        };
        const context = yield* Layer.build(adapterLayer(config, api, files));
        const adapter = Context.get(context, TelegramAdapter);
        const gateway = yield* makeGateway([adapter], config);

        const message = Array.from(
          yield* gateway.incoming.pipe(Stream.take(1), Stream.runCollect),
        )[0];

        expect(message?.text).toBe("two files");
        expect(message?.files.map((file) => file.kind)).toEqual(["image", "audio"]);
        expect(savedKinds).toEqual(["image", "audio"]);
      }),
    ),
  );
});
