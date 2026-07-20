import { NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { Gateway, makeGateway } from "./Gateway.ts";
import { GatewayConfig, layerDefault as gatewayConfigLayerDefault } from "./GatewayConfig.ts";
import { layerDefault as gatewayFilesLayerDefault } from "./GatewayFiles.ts";
import { TelegramAdapter } from "./adapters/telegram/TelegramAdapter.ts";
import { TelegramApi } from "./adapters/telegram/TelegramApi.ts";

export {
  AdapterName,
  ChannelId,
  type GatewayAdapter,
  type GatewayAdapterError,
  GatewayAdapterNotFoundError,
  GatewayAuthorizationError,
  GatewayConfigurationError,
  type GatewayError,
  GatewayFile,
  GatewayFileError,
  GatewayFileKind,
  GatewayInvalidMessageError,
  GatewayMessageId,
  GatewayProtocolError,
  GatewayTimestampMillis,
  GatewayTransportError,
  type IncomingAdapterEvent,
  IncomingMessage,
  MessageSender,
  OutgoingMessage,
  type ParsedChannelId,
  SentMessage,
  ThreadId,
  parseChannelId,
} from "@compass/contracts";
export * from "./Gateway.ts";
export {
  GatewayConfig,
  type GatewayConfigFileOptions,
  type GatewaySettings,
  type TelegramGatewaySettings,
  layerDefault as gatewayConfigLayerDefault,
  layerFile as gatewayConfigLayerFile,
  loadGatewayConfig,
} from "./GatewayConfig.ts";
export {
  GatewayFiles,
  type GatewayFilesService,
  type SaveGatewayFileOptions,
  type SaveGatewayFileStreamOptions,
  layerAt as gatewayFilesLayerAt,
  layerDefault as gatewayFilesLayerDefault,
} from "./GatewayFiles.ts";
export * as Telegram from "./adapters/telegram/index.ts";

export const telegramGatewayLayer = Layer.effect(
  Gateway,
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const telegram = yield* TelegramAdapter;
    return yield* makeGateway([telegram], config);
  }),
);

export const telegramGatewayLayerLive = telegramGatewayLayer.pipe(
  Layer.provide(TelegramAdapter.layer),
  Layer.provide(TelegramApi.layer),
  Layer.provide(gatewayConfigLayerDefault),
  Layer.provide(gatewayFilesLayerDefault),
  Layer.provide(FetchHttpClient.layer),
  Layer.provide(NodeServices.layer),
);
