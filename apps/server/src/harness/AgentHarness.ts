import {
  type CreateSessionOptions,
  EventBufferSize,
  ModelKey,
  ProviderKey,
  type SessionId,
  type HarnessError,
  SessionNotFoundError,
  SessionPersistenceError,
  SessionConfiguration,
  StoredSession,
  TokenLimit,
} from "@compass/contracts";
import { Context, Crypto, Effect, Layer, Option, Ref, Scope, Semaphore } from "effect";
import { AgentSession } from "./AgentSession.ts";
import { Compactor } from "./Compaction.ts";
import { Instructions } from "./Instructions.ts";
import { ConversationModel } from "./Models.ts";
import { SessionStore } from "./SessionStore.ts";
import { now } from "./internal/Ids.ts";
import { TokenCounter } from "./internal/TokenCounter.ts";

export class AgentHarness extends Context.Service<
  AgentHarness,
  {
    readonly create: (
      options?: CreateSessionOptions,
    ) => Effect.Effect<AgentSession["Service"], HarnessError>;
    readonly open: (sessionId: SessionId) => Effect.Effect<AgentSession["Service"], HarnessError>;
  }
>()("@compass/server/harness/AgentHarness") {
  static readonly layer = Layer.effect(
    AgentHarness,
    Effect.gen(function* () {
      const store = yield* SessionStore;
      const binding = yield* ConversationModel;
      const crypto = yield* Crypto.Crypto;
      const sessionEnvironment = yield* Effect.context<
        | Compactor
        | ConversationModel
        | Crypto.Crypto
        | Instructions
        | Scope.Scope
        | SessionStore
        | TokenCounter
      >();
      const sessions = yield* Ref.make(new Map<SessionId, AgentSession["Service"]>());
      const semaphore = yield* Semaphore.make(1);

      const materialize = Effect.fn("AgentHarness.materialize")(function* (stored: StoredSession) {
        const existing = (yield* Ref.get(sessions)).get(stored.id);
        if (existing !== undefined) return existing;
        const context = yield* Layer.build(AgentSession.layer(stored)).pipe(
          Effect.provideContext(sessionEnvironment),
          Effect.mapError(
            (cause) =>
              new SessionPersistenceError({
                operation: "open session runtime",
                message: "Could not open the session runtime",
                cause,
              }),
          ),
        );
        const agentSession = Context.get(context, AgentSession);
        yield* Ref.update(sessions, (current) => new Map(current).set(stored.id, agentSession));
        return agentSession;
      }, semaphore.withPermits(1));

      const open = Effect.fn("AgentHarness.open")(function* (sessionId: SessionId) {
        const stored = yield* store.getSession(sessionId);
        if (Option.isNone(stored)) {
          return yield* new SessionNotFoundError({ sessionId });
        }
        return yield* materialize(stored.value);
      });

      const create = Effect.fn("AgentHarness.create")(function* (options?: CreateSessionOptions) {
        const [generatedId, timestamp] = yield* Effect.all([
          crypto.randomUUIDv7.pipe(
            Effect.map((value) => StoredSession.fields.id.make(value)),
            Effect.mapError(
              (cause) =>
                new SessionPersistenceError({
                  operation: "create session id",
                  message: "Could not create a session identifier",
                  cause,
                }),
            ),
          ),
          now,
        ]);
        const id = options?.id ?? generatedId;
        const stored = new StoredSession({
          id,
          configuration: new SessionConfiguration({
            provider: options?.provider ?? ProviderKey.make(binding.provider),
            model: options?.model ?? ModelKey.make(binding.model),
            systemInstructions: options?.systemInstructions ?? "",
            compactAtTokens: options?.compactAtTokens ?? TokenLimit.make(100_000),
            summaryMaxTokens: options?.summaryMaxTokens ?? TokenLimit.make(4_000),
            eventBufferSize: options?.eventBufferSize ?? EventBufferSize.make(256),
          }),
          createdAt: timestamp,
          updatedAt: timestamp,
        });
        yield* store.createSession(stored);
        return yield* materialize(stored);
      });

      return AgentHarness.of({ create, open });
    }),
  );
}
