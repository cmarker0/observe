import {
  createTelemetrySanitizer,
  sanitizeTelemetryBatch,
  sanitizeTelemetryEntry,
  SECTION_SHAPES,
} from "./telemetry-wire-contract.js";

/**
 * The sanitizer is the runtime half of the wire contract: the collector
 * validates with `forbidNonWhitelisted` and answers a 400 for the whole batch
 * over one bad entry, so whatever it would refuse has to be repaired or
 * dropped - entry by entry - before the batch leaves the process.
 */
describe("sanitizeTelemetryEntry", () => {
  describe("request snapshots", () => {
    const snapshot = (
      overrides: Record<string, unknown> = {},
    ): Record<string, unknown> => ({
      ti: "trace-1",
      op: "/users/:id",
      d: 12.5,
      ...overrides,
    });

    it("accepts a well-formed snapshot untouched", () => {
      const entry = snapshot();

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result).toEqual({ ok: true, problems: [] });
      expect(entry).toEqual(snapshot());
    });

    it("strips a leaked start timestamp instead of losing the snapshot", () => {
      // `st` is kept off the wire by `endTrace` deleting `startTimestamp`, but
      // any path that skips `endTrace` leaks it - and the API forbids
      // undeclared keys, batch-wide.
      const entry = snapshot({ st: 123.45 });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(result.problems).toEqual([
        "snapshots.st: not declared by the contract, removed",
      ]);
      expect(entry).not.toHaveProperty("st");
    });

    it("reports a snapshot without a trace id as unsalvageable", () => {
      const result = sanitizeTelemetryEntry("snapshots", snapshot({ ti: undefined }));

      expect(result.ok).toBe(false);
      expect(result.problems).toEqual(["snapshots.ti: required"]);
    });

    it("removes a mistyped optional field rather than the entry", () => {
      const entry = snapshot({ d: "12ms" });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry).not.toHaveProperty("d");
    });

    it("removes an explicit null rather than sending it", () => {
      // Optional means absent is fine; a present null still fails the API's
      // type check.
      const entry = snapshot({ u: null });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry).not.toHaveProperty("u");
    });

    it("removes a non-finite duration, which would arrive as null", () => {
      const entry = snapshot({ d: NaN });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry).not.toHaveProperty("d");
    });

    it("sanitizes trace nodes recursively", () => {
      const entry = snapshot({
        t: [
          {
            n: "span",
            d: 1,
            leaked: true,
            ch: [{ n: "child", startTimestamp: 99 }],
          },
        ],
      });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry.t).toEqual([{ n: "span", d: 1, ch: [{ n: "child" }] }]);
    });

    it("keeps a boolean error flag on a trace node", () => {
      // `e: true` is the "failed, but handled - details not captured" form the
      // registry emits for every failed non-root span; stripping it would
      // erase real error signals.
      const entry = snapshot({
        e: { message: "root failed" },
        t: [{ n: "child", e: true }],
      });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result).toEqual({ ok: true, problems: [] });
      expect(entry.t).toEqual([{ n: "child", e: true }]);
      expect(entry.e).toEqual({ message: "root failed" });
    });

    it("removes an unusable array element, keeping its siblings", () => {
      const entry = snapshot({ t: [{ n: "good" }, "not a span", { n: "also good" }] });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry.t).toEqual([{ n: "good" }, { n: "also good" }]);
    });

    it("strips undeclared attribute keys", () => {
      // `a` is a modelled shape, not a pass-through: only m/sc/ou are declared.
      const entry = snapshot({ a: { m: "GET", sc: 200, userAgent: "curl" } });

      const result = sanitizeTelemetryEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry.a).toEqual({ m: "GET", sc: 200 });
    });
  });

  describe("other sections", () => {
    it("requires a job id", () => {
      expect(sanitizeTelemetryEntry("jobs", { n: "send-mail" }).ok).toBe(false);
      expect(sanitizeTelemetryEntry("jobs", { i: "job-1" }).ok).toBe(true);
    });

    it("requires a custom metric name", () => {
      const result = sanitizeTelemetryEntry("custom", {
        t: "counter",
        v: { default: 1 },
      });

      expect(result.ok).toBe(false);
    });

    it.each([
      [
        "a labelled counter",
        () => ({
          n: "orders",
          t: "counter",
          v: { '{"region":"eu"}': 5 },
          l: ["region"],
          iv: { '{"region":"eu"}': 2 },
        }),
      ],
      [
        "a summary",
        () => ({
          n: "latency",
          t: "summary",
          q50: { default: 20 },
          q95: { default: 30 },
          q99: { default: 30 },
          ct: { default: 3 },
          sm: { default: 60 },
          mx: { default: 30 },
        }),
      ],
    ])("keeps every per-label reading of %s", (_label, sample) => {
      // Readings are maps from label to number, and `l` the label names.
      // Declared as numbers and an object, a repaired batch sent each metric
      // with nothing but its name.
      const entry = sample();

      const result = sanitizeTelemetryEntry("custom", entry);

      expect(result).toEqual({ ok: true, problems: [] });
      expect(entry).toEqual(sample());
    });

    it("drops a log entry with a non-finite timestamp", () => {
      // NaN survives `typeof` but serializes to null, and `timestamp` is
      // required - nothing to strip, so the entry goes.
      const result = sanitizeTelemetryEntry("logs", {
        timestamp: NaN,
        text: "boom",
      });

      expect(result.ok).toBe(false);
    });

    it("keeps a log entry whose attributes are not an object by stripping them", () => {
      const entry = {
        timestamp: 1,
        text: "hello",
        attributes: ["not", "an", "object"],
      };

      const result = sanitizeTelemetryEntry("logs", entry);

      expect(result.ok).toBe(true);
      expect(entry).not.toHaveProperty("attributes");
    });

    it("repairs runtime metrics by removing what does not conform", () => {
      const entry = {
        c: { u: 0.5, s: 0.1, p: 0.2 },
        m: "oops",
      };

      const result = sanitizeTelemetryEntry("runtime", entry);

      expect(result.ok).toBe(true);
      expect(entry).toEqual({ c: { u: 0.5, s: 0.1, p: 0.2 } });
    });

    it("keeps a runtime sample's GC breakdown", () => {
      // Each kind is a count and a duration, not a number. Declared as one,
      // every repaired batch lost its whole breakdown and blamed the kinds.
      const sample = () => ({
        g: {
          c: 4,
          td: 20,
          b: {
            m: { count: 3, duration: 12 },
            j: { count: 1, duration: 8 },
            i: { count: 0, duration: 0 },
          },
        },
      });
      const entry = sample();

      const result = sanitizeTelemetryEntry("runtime", entry);

      expect(result).toEqual({ ok: true, problems: [] });
      expect(entry).toEqual(sample());
    });
  });

  describe("whole batches", () => {
    it("drops only the unsalvageable entry, keeping the batch", () => {
      const payload: Record<string, unknown> = {
        serviceId: "svc",
        snapshots: [],
        logs: [
          { timestamp: 1, text: "kept" },
          // No text at all - required, nothing to strip, entry goes alone.
          { timestamp: 2 },
        ],
      };

      const result = sanitizeTelemetryBatch(payload);

      expect(result.changed).toBe(true);
      expect(result.dropped).toBe(1);
      expect(payload.logs).toEqual([{ timestamp: 1, text: "kept" }]);
    });

    it("removes unusable runtime metrics without touching the rest", () => {
      const payload: Record<string, unknown> = {
        serviceId: "svc",
        snapshots: [{ ti: "t-1", op: "/users" }],
        runtime: "corrupt",
      };

      const result = sanitizeTelemetryBatch(payload);

      expect(result.dropped).toBe(1);
      expect(payload).not.toHaveProperty("runtime");
      expect(payload.snapshots).toHaveLength(1);
    });

    it("reports a batch already in contract as unchanged", () => {
      // `changed: false` is what tells the worker a 400 was not the payload's
      // fault - nothing to repair, so nothing to re-send.
      const payload: Record<string, unknown> = {
        serviceId: "svc",
        snapshots: [{ ti: "t-1", op: "/users", d: 3 }],
        logs: [{ timestamp: 1, text: "hello" }],
      };

      const result = sanitizeTelemetryBatch(payload);

      expect(result).toEqual({ changed: false, dropped: 0, problems: [] });
    });
  });

  describe("self-containment", () => {
    it("survives stringify-and-eval, which is how the worker receives it", () => {
      // The detached worker gets the factory as source
      // (`createTelemetrySanitizer.toString()`) and the shapes through
      // `workerData`. A reference to anything in the contract module's scope
      // would pass every direct-call test and then throw inside the worker -
      // this test runs the factory the way the worker does.
      const rebuilt = new Function(
        `return (${createTelemetrySanitizer.toString()})`,
      )()(JSON.parse(JSON.stringify(SECTION_SHAPES)));

      const entry: Record<string, unknown> = { ti: "t-1", st: 123 };
      const result = rebuilt.sanitizeEntry("snapshots", entry);

      expect(result.ok).toBe(true);
      expect(entry).not.toHaveProperty("st");
    });
  });
});
