import { describe, expect, it } from "@effect/vitest";
import { ModelKey, PromptInput, ProviderKey, TokenLimit } from "@compass/contracts";
import { Context, Effect, Layer, Stream } from "effect";
import { LanguageModel, Prompt, type Response } from "effect/unstable/ai";
import { AgentHarness } from "./AgentHarness.ts";
import * as Harness from "./Harness.ts";
import { CompactionModel, ConversationModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";

const finishPart: Response.FinishPartEncoded = {
  type: "finish",
  reason: "stop",
  usage: {
    inputTokens: {
      uncached: 1,
      total: 1,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
};

const userMessage = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] });

const promptText = (message: Prompt.Message): string =>
  typeof message.content === "string"
    ? message.content
    : message.content
        .filter((part): part is Prompt.TextPart => part.type === "text")
        .map((part) => part.text)
        .join("");

describe("AgentHarness", () => {
  it.effect("automatically compacts history before the next model call", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const conversationPrompts: Array<Prompt.Prompt> = [];
        const compactionPrompts: Array<Prompt.Prompt> = [];
        let responseNumber = 0;
        const modelLayer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: (options) => {
              compactionPrompts.push(options.prompt);
              return Effect.succeed([
                { type: "text", text: "The user established prior context." },
                finishPart,
              ]);
            },
            streamText: (options) => {
              conversationPrompts.push(options.prompt);
              responseNumber += 1;
              return Stream.fromIterable([
                { type: "text-start", id: `text-${responseNumber}` },
                {
                  type: "text-delta",
                  id: `text-${responseNumber}`,
                  delta: `answer-${responseNumber}`,
                },
                { type: "text-end", id: `text-${responseNumber}` },
                finishPart,
              ] satisfies ReadonlyArray<Response.StreamPartEncoded>);
            },
          }),
        );
        const binding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("scripted"),
          layer: modelLayer,
        };
        const models = Layer.merge(
          ConversationModel.layer(binding),
          CompactionModel.layer(binding),
        );
        const context = yield* Layer.build(
          Harness.layer(models, { store: SessionStore.layerMemory }),
        );
        const harness = Context.get(context, AgentHarness);
        const session = yield* harness.create({
          compactAtTokens: TokenLimit.make(1),
        });

        yield* session.offer(new PromptInput({ message: userMessage("first request") }));
        yield* session.waitForIdle;
        yield* session.offer(new PromptInput({ message: userMessage("latest request") }));
        yield* session.waitForIdle;

        const secondPrompt = conversationPrompts[1];
        expect(compactionPrompts).toHaveLength(1);
        expect(secondPrompt).toBeDefined();
        expect(
          secondPrompt?.content.some((message) =>
            promptText(message).includes("Conversation summary"),
          ),
        ).toBe(true);
        expect(
          secondPrompt?.content.some((message) => promptText(message) === "latest request"),
        ).toBe(true);
        expect(
          secondPrompt?.content.some((message) => promptText(message) === "first request"),
        ).toBe(false);
      }),
    ),
  );
});
