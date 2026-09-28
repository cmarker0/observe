import { SkipSpansContext } from "../interfaces/observe-options.interface.js";
import { resolveSkipSpans } from "./skip-spans.util.js";

describe("resolveSkipSpans", () => {
  const operation = (
    overrides: Partial<SkipSpansContext> = {},
  ): SkipSpansContext => ({
    protocol: "http",
    operationId: "/contacts/:id",
    method: "GET",
    statusCode: 404,
    duration: 3,
    errorClass: "NotFoundException",
    ...overrides,
  });

  it("resolves to nothing when the option is absent or empty", () => {
    // Nothing to ask means the registry never builds a context at all.
    expect(resolveSkipSpans(undefined)).toBeUndefined();
    expect(resolveSkipSpans([])).toBeUndefined();
  });

  it("matches a list on the status code alone", () => {
    const skips = resolveSkipSpans([400, 401, 403, 404])!;

    expect(skips(operation({ statusCode: 404 }))).toBe(true);
    expect(skips(operation({ statusCode: 401, errorClass: undefined }))).toBe(
      true,
    );
    expect(skips(operation({ statusCode: 409 }))).toBe(false);
    expect(skips(operation({ statusCode: 500 }))).toBe(false);
  });

  it("never matches an operation reported without a status", () => {
    const skips = resolveSkipSpans([404])!;

    expect(skips(operation({ statusCode: undefined }))).toBe(false);
  });

  it("does not coerce: a status written as a string matches nothing", () => {
    const skips = resolveSkipSpans(["404"] as unknown as number[])!;

    expect(skips(operation({ statusCode: 404 }))).toBe(false);
  });

  it("hands a function through as the rule", () => {
    const rule = (op: SkipSpansContext) =>
      op.errorClass === "NotFoundException";

    expect(resolveSkipSpans(rule)).toBe(rule);
  });
});
