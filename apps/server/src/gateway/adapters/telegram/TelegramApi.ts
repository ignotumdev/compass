import {
  AdapterName,
  GatewayProtocolError,
  GatewayTransportError,
  type GatewayAdapterError,
} from "@compass/contracts";
import { Config, Context, Effect, Layer, Redacted, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import {
  type TelegramFile,
  TelegramFile as TelegramFileSchema,
  type TelegramMessage,
  TelegramMessage as TelegramMessageSchema,
  type TelegramSendMediaMethod,
  type TelegramUser,
  TelegramUser as TelegramUserSchema,
} from "./TelegramModels.ts";

const TELEGRAM_ADAPTER = AdapterName.make("telegram");
const DEFAULT_API_BASE_URL = "https://api.telegram.org";

export interface TelegramPollingRequest {
  readonly offset?: number | undefined;
  readonly timeout: number;
  readonly limit: number;
}

export interface TelegramMediaRequest {
  readonly method: TelegramSendMediaMethod;
  readonly field: "photo" | "audio" | "voice" | "video" | "document";
  readonly chatId: string;
  readonly threadId?: number | undefined;
  readonly filename: string;
  readonly mediaType?: string | undefined;
  readonly bytes: Uint8Array;
  readonly caption?: string | undefined;
}

export interface TelegramApiService {
  readonly getMe: Effect.Effect<TelegramUser, GatewayAdapterError>;
  readonly deleteWebhook: (dropPendingUpdates: boolean) => Effect.Effect<void, GatewayAdapterError>;
  readonly getUpdates: (
    request: TelegramPollingRequest,
  ) => Effect.Effect<ReadonlyArray<unknown>, GatewayAdapterError>;
  readonly getFile: (fileId: string) => Effect.Effect<TelegramFile, GatewayAdapterError>;
  readonly download: (filePath: string) => Stream.Stream<Uint8Array, GatewayAdapterError>;
  readonly sendText: (
    chatId: string,
    threadId: number | undefined,
    text: string,
  ) => Effect.Effect<TelegramMessage, GatewayAdapterError>;
  readonly sendMedia: (
    request: TelegramMediaRequest,
  ) => Effect.Effect<TelegramMessage, GatewayAdapterError>;
}

export interface TelegramApiOptions {
  readonly botToken: Redacted.Redacted;
  readonly apiBaseUrl: string;
}

export class TelegramApi extends Context.Service<TelegramApi, TelegramApiService>()(
  "@compass/server/gateway/adapters/telegram/TelegramApi",
) {
  static readonly layer = Layer.effect(
    TelegramApi,
    Effect.gen(function* () {
      const botToken = yield* Config.redacted("TELEGRAM_BOT_TOKEN");
      const apiBaseUrl = yield* Config.string("TELEGRAM_API_BASE_URL").pipe(
        Config.withDefault(DEFAULT_API_BASE_URL),
      );
      return yield* makeTelegramApi({ botToken, apiBaseUrl });
    }),
  );

  static readonly layerWith = (options: TelegramApiOptions) =>
    Layer.effect(TelegramApi, makeTelegramApi(options));
}

const apiEnvelope = <A, RD>(result: Schema.ConstraintDecoder<A, RD>) =>
  Schema.Struct({
    ok: Schema.Boolean,
    result: Schema.optionalKey(result),
    description: Schema.optionalKey(Schema.String),
    error_code: Schema.optionalKey(Schema.Finite),
    parameters: Schema.optionalKey(
      Schema.Struct({ retry_after: Schema.optionalKey(Schema.Finite) }),
    ),
  });

export const makeTelegramApi = Effect.fn("TelegramApi.make")(function* (
  options: TelegramApiOptions,
) {
  const client = yield* HttpClient.HttpClient;
  const baseUrl = options.apiBaseUrl.replace(/\/+$/, "");
  const token = Redacted.value(options.botToken);
  const methodUrl = (method: string) => `${baseUrl}/bot${token}/${method}`;

  const call = <A, RD>(
    operation: string,
    payload: unknown,
    result: Schema.ConstraintDecoder<A, RD>,
  ): Effect.Effect<A, GatewayAdapterError, RD> =>
    Effect.gen(function* () {
      const request = HttpClientRequest.post(methodUrl(operation)).pipe(
        HttpClientRequest.bodyJsonUnsafe(payload),
      );
      const response = yield* client.execute(request).pipe(
        Effect.mapError(
          () =>
            new GatewayTransportError({
              adapter: TELEGRAM_ADAPTER,
              operation,
              message: `Telegram ${operation} request failed`,
              cause: new Error("Telegram HTTP transport failed"),
            }),
        ),
      );
      const json = yield* response.json.pipe(
        Effect.mapError(
          () =>
            new GatewayProtocolError({
              adapter: TELEGRAM_ADAPTER,
              operation,
              message: `Telegram ${operation} returned an unreadable response`,
              cause: new Error("Telegram HTTP response body was unreadable"),
            }),
        ),
      );
      const envelope = yield* Schema.decodeUnknownEffect(apiEnvelope(result))(json).pipe(
        Effect.mapError(
          (cause) =>
            new GatewayProtocolError({
              adapter: TELEGRAM_ADAPTER,
              operation,
              message: `Telegram ${operation} returned an invalid response`,
              cause,
            }),
        ),
      );
      if (!envelope.ok || envelope.result === undefined) {
        const retryAfter = envelope.parameters?.retry_after;
        const retryDetail = retryAfter === undefined ? "" : `; retry after ${retryAfter}s`;
        return yield* new GatewayProtocolError({
          adapter: TELEGRAM_ADAPTER,
          operation,
          message: `${envelope.description ?? "Telegram API request failed"}${retryDetail}`,
          cause: new Error(`Telegram API error ${envelope.error_code ?? "unknown"}`),
        });
      }
      return envelope.result;
    });

  const callFormData = Effect.fn("TelegramApi.callFormData")(function* (
    request: TelegramMediaRequest,
  ) {
    const formData = new FormData();
    formData.set("chat_id", request.chatId);
    if (request.threadId !== undefined) {
      formData.set("message_thread_id", String(request.threadId));
    }
    if (request.caption !== undefined && request.caption.length > 0) {
      formData.set("caption", request.caption);
    }
    formData.set(
      request.field,
      new Blob([new Uint8Array(request.bytes).buffer], {
        type: request.mediaType ?? "application/octet-stream",
      }),
      request.filename,
    );
    const httpRequest = HttpClientRequest.post(methodUrl(request.method)).pipe(
      HttpClientRequest.bodyFormData(formData),
    );
    const response = yield* client.execute(httpRequest).pipe(
      Effect.mapError(
        () =>
          new GatewayTransportError({
            adapter: TELEGRAM_ADAPTER,
            operation: request.method,
            message: `Telegram ${request.method} request failed`,
            cause: new Error("Telegram HTTP transport failed"),
          }),
      ),
    );
    const json = yield* response.json.pipe(
      Effect.mapError(
        () =>
          new GatewayProtocolError({
            adapter: TELEGRAM_ADAPTER,
            operation: request.method,
            message: `Telegram ${request.method} returned an unreadable response`,
            cause: new Error("Telegram HTTP response body was unreadable"),
          }),
      ),
    );
    const envelope = yield* Schema.decodeUnknownEffect(apiEnvelope(TelegramMessageSchema))(
      json,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new GatewayProtocolError({
            adapter: TELEGRAM_ADAPTER,
            operation: request.method,
            message: `Telegram ${request.method} returned an invalid response`,
            cause,
          }),
      ),
    );
    if (!envelope.ok || envelope.result === undefined) {
      return yield* new GatewayProtocolError({
        adapter: TELEGRAM_ADAPTER,
        operation: request.method,
        message: envelope.description ?? `Telegram ${request.method} failed`,
        cause: new Error(`Telegram API error ${envelope.error_code ?? "unknown"}`),
      });
    }
    return envelope.result;
  });

  const download = (filePath: string) => {
    const request = HttpClientRequest.get(`${baseUrl}/file/bot${token}/${filePath}`);
    return Stream.unwrap(
      client.execute(request).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.map((response) =>
          response.stream.pipe(
            Stream.mapError(
              () =>
                new GatewayTransportError({
                  adapter: TELEGRAM_ADAPTER,
                  operation: "read file response",
                  message: "Could not read a Telegram attachment response",
                  cause: new Error("Telegram attachment response stream failed"),
                }),
            ),
          ),
        ),
        Effect.mapError(
          () =>
            new GatewayTransportError({
              adapter: TELEGRAM_ADAPTER,
              operation: "download file",
              message: "Could not download a Telegram attachment",
              cause: new Error("Telegram attachment request failed"),
            }),
        ),
      ),
    );
  };

  const deleteWebhook = Effect.fn("TelegramApi.deleteWebhook")(function* (
    dropPendingUpdates: boolean,
  ) {
    yield* call("deleteWebhook", { drop_pending_updates: dropPendingUpdates }, Schema.Boolean);
  });

  const getUpdates = (request: TelegramPollingRequest) =>
    call(
      "getUpdates",
      {
        allowed_updates: ["message", "channel_post"],
        limit: request.limit,
        offset: request.offset,
        timeout: request.timeout,
      },
      Schema.Array(Schema.Unknown),
    );

  const getFile = (fileId: string) => call("getFile", { file_id: fileId }, TelegramFileSchema);

  const sendText = (chatId: string, threadId: number | undefined, text: string) =>
    call(
      "sendMessage",
      { chat_id: chatId, message_thread_id: threadId, text },
      TelegramMessageSchema,
    );

  return TelegramApi.of({
    getMe: call("getMe", {}, TelegramUserSchema),
    deleteWebhook,
    getUpdates,
    getFile,
    download,
    sendText,
    sendMedia: callFormData,
  });
});
