import { Context, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { ChannelId, GatewayConfigurationError } from "./Models.ts";
import { compassDirectory } from "./Paths.ts";

const IntegerBetween = (minimum: number, maximum: number) =>
  Schema.Finite.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(minimum),
    Schema.isLessThanOrEqualTo(maximum),
  );
const PositiveInteger = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThan(0));

const TelegramSettingsFile = Schema.Struct({
  pollingTimeoutSeconds: Schema.optionalKey(IntegerBetween(0, 50)),
  pollingLimit: Schema.optionalKey(IntegerBetween(1, 100)),
  retryBaseDelayMillis: Schema.optionalKey(PositiveInteger),
  retryMaxDelayMillis: Schema.optionalKey(PositiveInteger),
  incomingBufferCapacity: Schema.optionalKey(PositiveInteger),
  maxInboundFileBytes: Schema.optionalKey(PositiveInteger),
  deleteWebhook: Schema.optionalKey(Schema.Boolean),
  dropPendingUpdates: Schema.optionalKey(Schema.Boolean),
});

const GatewayConfigFile = Schema.Struct({
  version: Schema.Literal(1),
  allowedChannels: Schema.Array(ChannelId),
  adapters: Schema.optionalKey(
    Schema.Struct({ telegram: Schema.optionalKey(TelegramSettingsFile) }),
  ),
});

export interface TelegramGatewaySettings {
  readonly pollingTimeoutSeconds: number;
  readonly pollingLimit: number;
  readonly retryBaseDelayMillis: number;
  readonly retryMaxDelayMillis: number;
  readonly incomingBufferCapacity: number;
  readonly maxInboundFileBytes: number;
  readonly deleteWebhook: boolean;
  readonly dropPendingUpdates: boolean;
}

export interface GatewaySettings {
  readonly allowedChannels: ReadonlySet<ChannelId>;
  readonly telegram: TelegramGatewaySettings;
}

export interface GatewayConfigFileOptions {
  readonly filename: string;
}

const defaults: TelegramGatewaySettings = {
  pollingTimeoutSeconds: 30,
  pollingLimit: 100,
  retryBaseDelayMillis: 1_000,
  retryMaxDelayMillis: 30_000,
  incomingBufferCapacity: 256,
  maxInboundFileBytes: 50 * 1024 * 1024,
  deleteWebhook: true,
  dropPendingUpdates: false,
};

export class GatewayConfig extends Context.Service<GatewayConfig, GatewaySettings>()(
  "@compass/server/gateway/GatewayConfig",
) {
  static readonly layer = (settings: GatewaySettings) => Layer.succeed(GatewayConfig, settings);
}

export const loadGatewayConfig = Effect.fn("GatewayConfig.load")(function* (
  options: GatewayConfigFileOptions,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const contents = yield* fileSystem.readFileString(options.filename).pipe(
    Effect.mapError(
      (cause) =>
        new GatewayConfigurationError({
          path: options.filename,
          message: "Could not read the gateway configuration",
          cause,
        }),
    ),
  );
  const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(GatewayConfigFile))(
    contents,
    { onExcessProperty: "error" },
  ).pipe(
    Effect.mapError(
      (cause) =>
        new GatewayConfigurationError({
          path: options.filename,
          message: "gateway.json is invalid",
          cause,
        }),
    ),
  );
  const telegram = decoded.adapters?.telegram;
  return {
    allowedChannels: new Set(decoded.allowedChannels),
    telegram: {
      pollingTimeoutSeconds: telegram?.pollingTimeoutSeconds ?? defaults.pollingTimeoutSeconds,
      pollingLimit: telegram?.pollingLimit ?? defaults.pollingLimit,
      retryBaseDelayMillis: telegram?.retryBaseDelayMillis ?? defaults.retryBaseDelayMillis,
      retryMaxDelayMillis: telegram?.retryMaxDelayMillis ?? defaults.retryMaxDelayMillis,
      incomingBufferCapacity: telegram?.incomingBufferCapacity ?? defaults.incomingBufferCapacity,
      maxInboundFileBytes: telegram?.maxInboundFileBytes ?? defaults.maxInboundFileBytes,
      deleteWebhook: telegram?.deleteWebhook ?? defaults.deleteWebhook,
      dropPendingUpdates: telegram?.dropPendingUpdates ?? defaults.dropPendingUpdates,
    },
  } satisfies GatewaySettings;
});

export const layerFile = (options: GatewayConfigFileOptions) =>
  Layer.effect(GatewayConfig, loadGatewayConfig(options));

export const layerDefault = Layer.unwrap(
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const directory = yield* compassDirectory;
    return layerFile({ filename: path.join(directory, "gateway.json") });
  }),
);
