import { describe, expect, it } from "@effect/vitest";
import {
  type HarnessModelBinding,
  ModelKey,
  PromptInput,
  ProviderKey,
  TokenLimit,
} from "@compass/contracts";
import { Context, Effect, Layer, Stream } from "effect";
import { LanguageModel, Prompt, type Response, Tokenizer } from "effect/unstable/ai";
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
        const compactionLimits: Array<TokenLimit> = [];
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
        const binding: HarnessModelBinding = {
          provider: ProviderKey.make("test"),
          model: ModelKey.make("scripted"),
          layer: modelLayer,
          transformGeneration: (effect, options) => {
            compactionLimits.push(options.maxOutputTokens);
            return effect;
          },
        };
        const models = Layer.merge(
          ConversationModel.layer(binding),
          CompactionModel.layer(binding),
        );
        const tokenizer = Tokenizer.make({
          tokenize: (prompt) => {
            const characters = prompt.content.flatMap((message) => Array.from(promptText(message)));
            return Effect.succeed(characters.map((_, index) => index));
          },
        });
        const context = yield* Layer.build(
          Harness.layer(models, {
            store: SessionStore.layerMemory,
            tokenizer: Layer.succeed(Tokenizer.Tokenizer, tokenizer),
          }),
        );
        const harness = Context.get(context, AgentHarness);
        const session = yield* harness.create({
          compactAtTokens: TokenLimit.make(1),
          summaryMaxTokens: TokenLimit.make(16),
        });
        const unsupportedBinding = yield* Effect.flip(
          harness.create({ provider: ProviderKey.make("not-installed") }),
        );
        expect(unsupportedBinding._tag).toBe("SessionConfigurationError");

        yield* session.offer(new PromptInput({ message: userMessage("first request") }));
        yield* session.waitForIdle;
        const duplicate = yield* Effect.flip(harness.create({ id: session.id }));
        expect(duplicate._tag).toBe("SessionPersistenceError");
        yield* session.offer(new PromptInput({ message: userMessage("latest request") }));
        yield* session.waitForIdle;

        const secondPrompt = conversationPrompts[1];
        expect(compactionPrompts).toHaveLength(1);
        expect(compactionLimits).toEqual([16]);
        expect(secondPrompt).toBeDefined();
        expect(
          secondPrompt?.content.some((message) =>
            promptText(message).includes("Conversation summary"),
          ),
        ).toBe(true);
        expect(
          secondPrompt?.content.some((message) =>
            promptText(message).includes("Conversation summary:\n\nThe user establi"),
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
