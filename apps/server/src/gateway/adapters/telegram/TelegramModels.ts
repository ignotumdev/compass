import { Schema } from "effect";

const TelegramFileFields = {
  file_id: Schema.String,
  file_unique_id: Schema.optionalKey(Schema.String),
  file_size: Schema.optionalKey(Schema.Finite),
};

export const TelegramUser = Schema.Struct({
  id: Schema.Finite,
  is_bot: Schema.Boolean,
  first_name: Schema.String,
  last_name: Schema.optionalKey(Schema.String),
  username: Schema.optionalKey(Schema.String),
});
export type TelegramUser = typeof TelegramUser.Type;

export const TelegramChat = Schema.Struct({
  id: Schema.Finite,
  type: Schema.Literals(["private", "group", "supergroup", "channel"]),
  title: Schema.optionalKey(Schema.String),
  first_name: Schema.optionalKey(Schema.String),
  last_name: Schema.optionalKey(Schema.String),
  username: Schema.optionalKey(Schema.String),
});
export type TelegramChat = typeof TelegramChat.Type;

export const TelegramFile = Schema.Struct({
  ...TelegramFileFields,
  file_path: Schema.optionalKey(Schema.String),
});
export type TelegramFile = typeof TelegramFile.Type;

export const TelegramPhotoSize = Schema.Struct({
  ...TelegramFileFields,
  width: Schema.Finite,
  height: Schema.Finite,
});
export type TelegramPhotoSize = typeof TelegramPhotoSize.Type;

const TelegramDocument = Schema.Struct({
  ...TelegramFileFields,
  file_name: Schema.optionalKey(Schema.String),
  mime_type: Schema.optionalKey(Schema.String),
});

const TelegramAudio = Schema.Struct({
  ...TelegramFileFields,
  duration: Schema.optionalKey(Schema.Finite),
  performer: Schema.optionalKey(Schema.String),
  title: Schema.optionalKey(Schema.String),
  file_name: Schema.optionalKey(Schema.String),
  mime_type: Schema.optionalKey(Schema.String),
});

const TelegramVideo = Schema.Struct({
  ...TelegramFileFields,
  width: Schema.optionalKey(Schema.Finite),
  height: Schema.optionalKey(Schema.Finite),
  duration: Schema.optionalKey(Schema.Finite),
  file_name: Schema.optionalKey(Schema.String),
  mime_type: Schema.optionalKey(Schema.String),
});

const TelegramVoice = Schema.Struct({
  ...TelegramFileFields,
  duration: Schema.optionalKey(Schema.Finite),
  mime_type: Schema.optionalKey(Schema.String),
});

export const TelegramMessage = Schema.Struct({
  message_id: Schema.Finite,
  message_thread_id: Schema.optionalKey(Schema.Finite),
  date: Schema.Finite,
  chat: TelegramChat,
  from: Schema.optionalKey(TelegramUser),
  sender_chat: Schema.optionalKey(TelegramChat),
  text: Schema.optionalKey(Schema.String),
  caption: Schema.optionalKey(Schema.String),
  photo: Schema.optionalKey(Schema.Array(TelegramPhotoSize)),
  audio: Schema.optionalKey(TelegramAudio),
  document: Schema.optionalKey(TelegramDocument),
  video: Schema.optionalKey(TelegramVideo),
  video_note: Schema.optionalKey(TelegramVideo),
  voice: Schema.optionalKey(TelegramVoice),
});
export type TelegramMessage = typeof TelegramMessage.Type;

export const TelegramUpdateIdentity = Schema.Struct({ update_id: Schema.Finite });

export const TelegramUpdate = Schema.Struct({
  update_id: Schema.Finite,
  message: Schema.optionalKey(TelegramMessage),
  channel_post: Schema.optionalKey(TelegramMessage),
});
export type TelegramUpdate = typeof TelegramUpdate.Type;

export type TelegramSendMediaMethod =
  | "sendPhoto"
  | "sendAudio"
  | "sendVoice"
  | "sendVideo"
  | "sendDocument";
