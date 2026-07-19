import { describe, expect, it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import { Context, Effect, FileSystem, Layer } from "effect";
import { loadGatewayConfig } from "./GatewayConfig.ts";

describe("GatewayConfig", () => {
  it.effect("loads strict gateway.json data and applies polling defaults", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const platform = yield* Layer.build(NodeServices.layer);
        const fileSystem = Context.get(platform, FileSystem.FileSystem);
        const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "gateway-config-" });
        const filename = `${directory}/gateway.json`;
        yield* fileSystem.writeFileString(
          filename,
          JSON.stringify({
            version: 1,
            allowedChannels: ["telegram:123", "telegram:-100456"],
            adapters: { telegram: { pollingLimit: 25 } },
          }),
        );

        const config = yield* loadGatewayConfig({ filename }).pipe(Effect.provide(platform));
        expect(Array.from(config.allowedChannels)).toEqual(["telegram:123", "telegram:-100456"]);
        expect(config.telegram.pollingLimit).toBe(25);
        expect(config.telegram.pollingTimeoutSeconds).toBe(30);
        expect(config.telegram.maxInboundFileBytes).toBe(50 * 1024 * 1024);
        expect(config.telegram.deleteWebhook).toBe(true);
      }),
    ),
  );

  it.effect("rejects unknown keys instead of silently accepting typos", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const platform = yield* Layer.build(NodeServices.layer);
        const fileSystem = Context.get(platform, FileSystem.FileSystem);
        const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "gateway-config-" });
        const filename = `${directory}/gateway.json`;
        yield* fileSystem.writeFileString(
          filename,
          JSON.stringify({ version: 1, allowedChannels: [], allowChannels: ["telegram:1"] }),
        );

        const error = yield* loadGatewayConfig({ filename }).pipe(
          Effect.provide(platform),
          Effect.flip,
        );
        expect(error._tag).toBe("GatewayConfigurationError");
        expect(error.message).toBe("gateway.json is invalid");
      }),
    ),
  );
});
