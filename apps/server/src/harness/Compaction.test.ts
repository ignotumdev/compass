import { describe, expect, it } from "@effect/vitest";
import { limitSummaryText } from "./Compaction.ts";

describe("Compaction", () => {
  it("caps summaries by the configured UTF-8 token estimate", () => {
    const limited = limitSummaryText("\u00e9".repeat(20), 3);

    expect(new TextEncoder().encode(limited).byteLength).toBeLessThanOrEqual(12);
    expect(limited).toBe("\u00e9".repeat(6));
  });

  it("does not split a multi-byte code point", () => {
    expect(limitSummaryText("\u{1f642}\u{1f642}", 1)).toBe("\u{1f642}");
  });
});
