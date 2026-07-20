import {
  AdapterName,
  ChannelId,
  type GatewayAdapter,
  type GatewayFile,
  GatewayFileError,
  type GatewayFileKind,
  GatewayMessageId,
  GatewayProtocolError,
  GatewayTimestampMillis,
  type IncomingAdapterEvent,
  IncomingMessage,
  MessageSender,
  type OutgoingMessage,
  SentMessage,
  ThreadId,
  parseChannelId,
} from "@compass/contracts";
import { Context, Effect, Layer, Queue, Schedule, Schema, Stream } from "effect";
import { GatewayConfig } from "../../GatewayConfig.ts";
import { GatewayFiles } from "../../GatewayFiles.ts";
import { TelegramApi, type TelegramMediaRequest } from "./TelegramApi.ts";
import {
  type TelegramMessage,
  type TelegramPhotoSize,
  TelegramUpdate,
  TelegramUpdateIdentity,
} from "./TelegramModels.ts";

const TELEGRAM_ADAPTER = AdapterName.make("telegram");
const EMPTY_POLL_DELAY = "25 millis";

interface TelegramAttachment {
  readonly fileId: string;
  readonly preferredName: string;
  readonly kind: GatewayFileKind;
  readonly mediaType?: string | undefined;
}

const bestPhoto = (
  photos: ReadonlyArray<TelegramPhotoSize> | undefined,
): TelegramPhotoSize | undefined => {
  let selected: TelegramPhotoSize | undefined;
  let area = -1;
  for (const photo of photos ?? []) {
    const candidateArea = photo.width * photo.height;
    if (candidateArea > area) {
      selected = photo;
      area = candidateArea;
    }
  }
  return selected;
};

const attachmentsOf = (message: TelegramMessage): ReadonlyArray<TelegramAttachment> => {
  const attachments: Array<TelegramAttachment> = [];
  const photo = bestPhoto(message.photo);
  if (photo !== undefined) {
    attachments.push({
      fileId: photo.file_id,
      preferredName: `photo-${message.message_id}.jpg`,
      kind: "image",
      mediaType: "image/jpeg",
    });
  }
  if (message.audio !== undefined) {
    attachments.push({
      fileId: message.audio.file_id,
      preferredName: message.audio.file_name ?? `audio-${message.message_id}`,
      kind: "audio",
      mediaType: message.audio.mime_type,
    });
  }
  if (message.voice !== undefined) {
    attachments.push({
      fileId: message.voice.file_id,
      preferredName: `voice-${message.message_id}.ogg`,
      kind: "audio",
      mediaType: message.voice.mime_type ?? "audio/ogg",
    });
  }
  if (message.video !== undefined) {
    attachments.push({
      fileId: message.video.file_id,
      preferredName: message.video.file_name ?? `video-${message.message_id}.mp4`,
      kind: "video",
      mediaType: message.video.mime_type ?? "video/mp4",
    });
  }
  if (message.video_note !== undefined) {
    attachments.push({
      fileId: message.video_note.file_id,
      preferredName: `video-note-${message.message_id}.mp4`,
      kind: "video",
      mediaType: message.video_note.mime_type ?? "video/mp4",
    });
  }
  if (message.document !== undefined) {
    attachments.push({
      fileId: message.document.file_id,
      preferredName: message.document.file_name ?? `document-${message.message_id}`,
      kind: "file",
      mediaType: message.document.mime_type,
    });
  }
  return attachments;
};

const hasSupportedContent = (message: TelegramMessage): boolean =>
  message.text !== undefined || message.caption !== undefined || attachmentsOf(message).length > 0;

const senderOf = (message: TelegramMessage): MessageSender => {
  if (message.from !== undefined) {
    const displayName = [message.from.first_name, message.from.last_name]
      .filter((part) => part !== undefined && part.length > 0)
      .join(" ");
    return new MessageSender({
      id: String(message.from.id),
      displayName: displayName || message.from.username || String(message.from.id),
      ...(message.from.username === undefined ? {} : { username: message.from.username }),
      isBot: message.from.is_bot,
    });
  }
  const senderChat = message.sender_chat ?? message.chat;
  return new MessageSender({
    id: String(senderChat.id),
    displayName:
      senderChat.title ?? senderChat.username ?? senderChat.first_name ?? String(senderChat.id),
    ...(senderChat.username === undefined ? {} : { username: senderChat.username }),
    isBot: false,
  });
};

const inferMediaType = (file: GatewayFile): string | undefined => {
  if (file.mediaType !== undefined) {
    return file.mediaType;
  }
  const lower = file.name.toLowerCase();
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
  if (lower.endsWith(".png")) return "image/png";
  if (lower.endsWith(".gif")) return "image/gif";
  if (lower.endsWith(".mp3")) return "audio/mpeg";
  if (lower.endsWith(".ogg") || lower.endsWith(".opus")) return "audio/ogg";
  if (lower.endsWith(".mp4")) return "video/mp4";
  return undefined;
};

const mediaTarget = (file: GatewayFile): Pick<TelegramMediaRequest, "method" | "field"> => {
  const mediaType = inferMediaType(file);
  if (file.kind === "image") return { method: "sendPhoto", field: "photo" };
  if (file.kind === "video") return { method: "sendVideo", field: "video" };
  if (file.kind === "audio" && mediaType === "audio/ogg") {
    return { method: "sendVoice", field: "voice" };
  }
  if (file.kind === "audio") return { method: "sendAudio", field: "audio" };
  return { method: "sendDocument", field: "document" };
};

const parseThread = (
  message: OutgoingMessage,
): Effect.Effect<number | undefined, GatewayProtocolError> =>
  Effect.suspend(() => {
    if (message.thread === undefined) {
      return Effect.as(Effect.void, undefined as number | undefined);
    }
    const value = Number(message.thread);
    return Number.isSafeInteger(value) && value > 0
      ? Effect.succeed(value)
      : Effect.fail(
          new GatewayProtocolError({
            adapter: TELEGRAM_ADAPTER,
            operation: "parse thread",
            message: `Telegram thread ${message.thread} is not a positive integer`,
            cause: new Error("Invalid Telegram message_thread_id"),
          }),
        );
  });

export class TelegramAdapter extends Context.Service<TelegramAdapter, GatewayAdapter>()(
  "@compass/server/gateway/adapters/telegram/TelegramAdapter",
) {
  static readonly layer = Layer.effect(
    TelegramAdapter,
    Effect.suspend(() => makeTelegramAdapter),
  );
}

export const makeTelegramAdapter = Effect.gen(function* () {
  const api = yield* TelegramApi;
  const files = yield* GatewayFiles;
  const config = yield* GatewayConfig;
  const settings = config.telegram;
  const me = yield* api.getMe;
  yield* Effect.logInfo("Telegram gateway adapter authenticated", {
    botUserId: me.id,
    username: me.username,
  });
  if (settings.deleteWebhook) {
    yield* api.deleteWebhook(settings.dropPendingUpdates);
  }

  const queue = yield* Queue.bounded<IncomingAdapterEvent>(settings.incomingBufferCapacity);
  yield* Effect.addFinalizer(() => Queue.shutdown(queue));

  const materialize = Effect.fn("TelegramAdapter.materialize")(function* (
    message: TelegramMessage,
    channel: ChannelId,
  ) {
    const saved = yield* Effect.forEach(
      attachmentsOf(message),
      Effect.fn(function* (attachment) {
        const telegramFile = yield* api.getFile(attachment.fileId);
        if (telegramFile.file_path === undefined) {
          return yield* new GatewayProtocolError({
            adapter: TELEGRAM_ADAPTER,
            operation: "getFile",
            message: `Telegram did not return a path for file ${attachment.fileId}`,
            cause: new Error("Telegram file_path is missing"),
          });
        }
        if (
          telegramFile.file_size !== undefined &&
          telegramFile.file_size > settings.maxInboundFileBytes
        ) {
          return yield* new GatewayFileError({
            operation: "limit incoming file",
            path: telegramFile.file_path,
            message: `Incoming attachment exceeds the ${settings.maxInboundFileBytes} byte limit`,
            cause: new Error("Telegram attachment is too large"),
          });
        }
        return yield* files.saveStream({
          stream: api.download(telegramFile.file_path),
          preferredName: attachment.preferredName,
          kind: attachment.kind,
          mediaType: attachment.mediaType,
          maxBytes: settings.maxInboundFileBytes,
        });
      }),
      { concurrency: 2 },
    );
    return new IncomingMessage({
      id: GatewayMessageId.make(String(message.message_id)),
      channel,
      ...(message.message_thread_id === undefined
        ? {}
        : { thread: ThreadId.make(String(message.message_thread_id)) }),
      sender: senderOf(message),
      ...(message.text === undefined && message.caption === undefined
        ? {}
        : { text: message.text ?? message.caption }),
      files: saved,
      receivedAt: GatewayTimestampMillis.make(Math.max(0, Math.trunc(message.date * 1_000))),
    });
  });

  const processUpdate = Effect.fn("TelegramAdapter.processUpdate")(function* (input: unknown) {
    const identity = yield* Schema.decodeUnknownEffect(TelegramUpdateIdentity)(input).pipe(
      Effect.option,
    );
    if (identity._tag === "None") {
      yield* Effect.logWarning("Ignoring Telegram update without a valid update_id");
      return undefined;
    }
    const decoded = yield* Schema.decodeUnknownEffect(TelegramUpdate)(input).pipe(Effect.option);
    if (decoded._tag === "None") {
      yield* Effect.logWarning("Ignoring malformed Telegram update", {
        updateId: identity.value.update_id,
      });
      return identity.value.update_id;
    }
    const message = decoded.value.message ?? decoded.value.channel_post;
    if (message === undefined || !hasSupportedContent(message)) {
      return identity.value.update_id;
    }
    const channel = ChannelId.make(`telegram:${message.chat.id}`);
    yield* Queue.offer(queue, {
      channel,
      materialize: materialize(message, channel),
    });
    return identity.value.update_id;
  });

  const retrySchedule = Schedule.min([
    Schedule.exponential(settings.retryBaseDelayMillis),
    Schedule.spaced(Math.max(settings.retryBaseDelayMillis, settings.retryMaxDelayMillis)),
  ]).pipe(
    Schedule.tap(({ input }) =>
      Effect.logWarning("Telegram polling failed; retrying", { error: input }),
    ),
  );

  const polling = Effect.gen(function* () {
    let offset: number | undefined;
    while (true) {
      const updates = yield* api
        .getUpdates({
          offset,
          timeout: settings.pollingTimeoutSeconds,
          limit: settings.pollingLimit,
        })
        .pipe(Effect.retry(retrySchedule));
      for (const update of updates) {
        const updateId = yield* processUpdate(update);
        if (updateId !== undefined) {
          offset = Math.max(offset ?? 0, updateId + 1);
        }
      }
      if (updates.length === 0) {
        yield* Effect.sleep(EMPTY_POLL_DELAY);
      }
    }
  });
  yield* polling.pipe(Effect.forkScoped({ startImmediately: true }));

  const send = Effect.fn("TelegramAdapter.send")(function* (message: OutgoingMessage) {
    const { nativeChannel } = parseChannelId(message.channel);
    const threadId = yield* parseThread(message);
    const messageIds: Array<GatewayMessageId> = [];
    if (message.files.length === 0) {
      const sent = yield* api.sendText(nativeChannel, threadId, message.text ?? "");
      messageIds.push(GatewayMessageId.make(String(sent.message_id)));
    } else {
      for (let index = 0; index < message.files.length; index += 1) {
        const file = message.files[index];
        if (file === undefined) continue;
        const bytes = yield* files.read(file);
        const target = mediaTarget(file);
        const sent = yield* api.sendMedia({
          ...target,
          chatId: nativeChannel,
          threadId,
          filename: file.name,
          mediaType: inferMediaType(file),
          bytes,
          caption: index === 0 ? message.text : undefined,
        });
        messageIds.push(GatewayMessageId.make(String(sent.message_id)));
      }
    }
    return new SentMessage({
      channel: message.channel,
      ...(message.thread === undefined ? {} : { thread: message.thread }),
      messageIds,
    });
  });

  return TelegramAdapter.of({
    name: TELEGRAM_ADAPTER,
    incoming: Stream.fromQueue(queue),
    send,
  });
});
