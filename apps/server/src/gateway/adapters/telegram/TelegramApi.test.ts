import { describe, expect, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { makeTelegramApi } from "./TelegramApi.ts";

describe("TelegramApi", () => {
  it.effect("sends local media as multipart form data through Effect HttpClient", () =>
    Effect.gen(function* () {
      let requestedUrl = "";
      let requestedForm: FormData | undefined;
      const client = HttpClient.make((request) => {
        requestedUrl = request.url;
        if (request.body._tag === "FormData") {
          requestedForm = request.body.formData;
        }
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                ok: true,
                result: {
                  message_id: 7,
                  date: 1,
                  chat: { id: 123, type: "private" },
                },
              }),
              { headers: { "content-type": "application/json" } },
            ),
          ),
        );
      });
      const api = yield* makeTelegramApi({
        botToken: Redacted.make("secret-token"),
        apiBaseUrl: "https://telegram.invalid/",
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const response = yield* api.sendMedia({
        method: "sendPhoto",
        field: "photo",
        chatId: "123",
        threadId: 9,
        filename: "image.png",
        mediaType: "image/png",
        bytes: Uint8Array.of(1, 2, 3),
        caption: "caption",
      });

      expect(response.message_id).toBe(7);
      expect(requestedUrl).toBe("https://telegram.invalid/botsecret-token/sendPhoto");
      expect(requestedForm?.get("chat_id")).toBe("123");
      expect(requestedForm?.get("message_thread_id")).toBe("9");
      expect(requestedForm?.get("caption")).toBe("caption");
      expect(requestedForm?.get("photo")).toBeInstanceOf(File);
    }),
  );

  it.effect("maps unsuccessful Bot API envelopes to typed protocol errors", () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({ ok: false, error_code: 401, description: "Unauthorized" }),
            ),
          ),
        ),
      );
      const api = yield* makeTelegramApi({
        botToken: Redacted.make("bad-token"),
        apiBaseUrl: "https://telegram.invalid",
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const error = yield* api.getMe.pipe(Effect.flip);
      expect(error._tag).toBe("GatewayProtocolError");
      expect(error.operation).toBe("getMe");
      expect(error.message).toBe("Unauthorized");
    }),
  );
});
