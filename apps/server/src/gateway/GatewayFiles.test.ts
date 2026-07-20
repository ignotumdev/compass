import { describe, expect, it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import { Context, Effect, FileSystem, Layer, Stream } from "effect";
import { GatewayFiles, layerAt } from "./GatewayFiles.ts";

describe("GatewayFiles", () => {
  it.effect("atomically saves sanitized inbound files and reads them back", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const platform = yield* Layer.build(NodeServices.layer);
        const fileSystem = Context.get(platform, FileSystem.FileSystem);
        const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "gateway-files-" });
        const context = yield* Layer.build(
          layerAt(directory).pipe(Layer.provide(NodeServices.layer)),
        );
        const files = Context.get(context, GatewayFiles);
        const stored = yield* files.save({
          bytes: Uint8Array.of(1, 2, 3),
          preferredName: "../../unsafe name.png",
          kind: "image",
          mediaType: "image/png",
        });

        expect(stored.name).toBe("unsafe_name.png");
        expect(stored.path.startsWith(directory)).toBe(true);
        expect(stored.path.endsWith(".partial")).toBe(false);
        expect(Array.from(yield* files.read(stored))).toEqual([1, 2, 3]);
        expect((yield* fileSystem.readDirectory(directory)).length).toBe(1);

        const oversized = yield* files
          .saveStream({
            stream: Stream.make(Uint8Array.of(1, 2), Uint8Array.of(3, 4)),
            preferredName: "too-large.bin",
            kind: "file",
            maxBytes: 3,
          })
          .pipe(Effect.flip);
        expect(oversized._tag).toBe("GatewayFileError");
        expect(oversized.operation).toBe("limit incoming file");
        expect((yield* fileSystem.readDirectory(directory)).length).toBe(1);
      }),
    ),
  );
});
