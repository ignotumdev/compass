# Compass

Compass is a minimal AI agent designed to help users with a variety of tasks. It is built on small primitives.

## Task Completion Requirements

- `vp run ready` and `vp run typecheck` and must pass before considering tasks completed.

## Core Priorities

1. Performance first.
2. Reliability first.
3. Keep behavior predictable under load and during failures (session restarts, reconnects, partial streams).

If a tradeoff is required, choose correctness and robustness over short-term convenience.

## Maintainability

This repository is still very early and under active development. Proposing sweeping changes that improve long-term maintainability is encouraged. You have standing permission to make breaking changes, without writing migrations for them.

Long term maintainability is a core priority. If you add new functionality, first check if there is shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.

## Effect

This codebase uses Effect v4. When writing Effect code, inspect .repos/effect/ for how to use it, as it is not included in your training data. The aim is to make everything as Effect-native as possible. Look for examples of idiomatic usage, tests, module structure, and API design. Treat it as the source of truth for Effect patterns. Always read .repos/effect/LLMS.md before writing any Effect code.

Instead of creating your own solution, search the Effect codebase, as the Effect standard library will most likely already contain what you need. Prefer examples and patterns from the vendored source code over generated guesses or web search results.
