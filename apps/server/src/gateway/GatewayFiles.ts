import { GatewayFile, GatewayFileError, type GatewayFileKind } from "@compass/contracts";
import { Context, Crypto, Effect, FileSystem, Layer, Path, Schema, Stream } from "effect";
import { compassDirectory } from "./Paths.ts";

const UNSAFE_FILENAME_CHARACTERS = /[^a-zA-Z0-9._-]+/g;
const LEADING_DOTS = /^\.+/;
const MAX_FILENAME_LENGTH = 160;

export interface SaveGatewayFileOptions {
  readonly bytes: Uint8Array;
  readonly preferredName: string;
  readonly kind: GatewayFileKind;
  readonly mediaType?: string | undefined;
}

export interface SaveGatewayFileStreamOptions<E, R> {
  readonly stream: Stream.Stream<Uint8Array, E, R>;
  readonly preferredName: string;
  readonly kind: GatewayFileKind;
  readonly mediaType?: string | undefined;
  readonly maxBytes: number;
}

export interface GatewayFilesService {
  readonly directory: string;
  readonly save: (options: SaveGatewayFileOptions) => Effect.Effect<GatewayFile, GatewayFileError>;
  readonly saveStream: <E, R>(
    options: SaveGatewayFileStreamOptions<E, R>,
  ) => Effect.Effect<GatewayFile, GatewayFileError, R>;
  readonly read: (file: GatewayFile) => Effect.Effect<Uint8Array, GatewayFileError>;
}

const sanitizeFilename = (path: Path.Path, input: string): string => {
  const basename = path.basename(input).replace(UNSAFE_FILENAME_CHARACTERS, "_");
  const withoutLeadingDots = basename.replace(LEADING_DOTS, "");
  return (withoutLeadingDots || "file").slice(0, MAX_FILENAME_LENGTH);
};

export class GatewayFiles extends Context.Service<GatewayFiles, GatewayFilesService>()(
  "@compass/server/gateway/GatewayFiles",
) {
  static readonly layer = (service: GatewayFilesService) => Layer.succeed(GatewayFiles, service);
}

const make = Effect.fn("GatewayFiles.make")(function* (directory: string) {
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory(directory, { recursive: true }).pipe(
    Effect.mapError(
      (cause) =>
        new GatewayFileError({
          operation: "create directory",
          path: directory,
          message: "Could not create the Compass gateway files directory",
          cause,
        }),
    ),
  );

  const saveStream = Effect.fn("GatewayFiles.saveStream")(function* <E, R>(
    options: SaveGatewayFileStreamOptions<E, R>,
  ): Effect.fn.Return<GatewayFile, GatewayFileError, R> {
    const identifier = yield* crypto.randomUUIDv7.pipe(
      Effect.mapError(
        (cause) =>
          new GatewayFileError({
            operation: "generate file name",
            path: directory,
            message: "Could not generate a unique attachment name",
            cause,
          }),
      ),
    );
    const filename = `${identifier}-${sanitizeFilename(path, options.preferredName)}`;
    const destination = path.join(directory, filename);
    const temporary = `${destination}.partial`;
    let size = 0;
    yield* options.stream.pipe(
      Stream.mapEffect((chunk) =>
        Effect.suspend(() => {
          size += chunk.byteLength;
          return size <= options.maxBytes
            ? Effect.succeed(chunk)
            : Effect.fail(
                new GatewayFileError({
                  operation: "limit incoming file",
                  path: destination,
                  message: `Incoming attachment exceeds the ${options.maxBytes} byte limit`,
                  cause: new Error("Incoming gateway attachment is too large"),
                }),
              );
        }),
      ),
      Stream.run(fileSystem.sink(temporary)),
      Effect.andThen(fileSystem.rename(temporary, destination)),
      Effect.onError(() => fileSystem.remove(temporary, { force: true }).pipe(Effect.ignore)),
      Effect.mapError((cause) =>
        Schema.is(GatewayFileError)(cause)
          ? cause
          : new GatewayFileError({
              operation: "save incoming file",
              path: destination,
              message: "Could not persist an incoming gateway attachment",
              cause,
            }),
      ),
    );
    return new GatewayFile({
      path: destination,
      name: sanitizeFilename(path, options.preferredName),
      kind: options.kind,
      ...(options.mediaType === undefined ? {} : { mediaType: options.mediaType }),
      size,
    });
  });

  const save = (options: SaveGatewayFileOptions) =>
    saveStream({
      stream: Stream.succeed(options.bytes),
      preferredName: options.preferredName,
      kind: options.kind,
      mediaType: options.mediaType,
      maxBytes: options.bytes.byteLength,
    });

  const read = Effect.fn("GatewayFiles.read")(function* (file: GatewayFile) {
    return yield* fileSystem.readFile(file.path).pipe(
      Effect.mapError(
        (cause) =>
          new GatewayFileError({
            operation: "read outgoing file",
            path: file.path,
            message: `Could not read outgoing attachment ${file.name}`,
            cause,
          }),
      ),
    );
  });

  return GatewayFiles.of({ directory, save, saveStream, read });
});

export const layerAt = (directory: string) => Layer.effect(GatewayFiles, make(directory));

export const layerDefault = Layer.unwrap(
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const directory = yield* compassDirectory;
    return layerAt(path.join(directory, "files"));
  }),
);
