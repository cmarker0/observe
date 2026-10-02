import { Counter } from "../custom-metrics/counter.js";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { ObserveAgentSharedBuffer } from "./observe-agent.shared-buffer.js";

const MAX_LOGS_PER_TRANSACTION = 250;
const MAX_PENDING_LOGS = 2000;
const MAX_LOG_ENTRY_LENGTH = 4 * 1024;

function createBuffer(
  overrides: Partial<ObserveModuleOptionsWithDefaults> = {},
) {
  return new ObserveAgentSharedBuffer({
    serviceId: "svc-test",
    forwardLogs: true,
    ...overrides,
  } as ObserveModuleOptionsWithDefaults);
}

const logs = (count: number, text = "line") =>
  Array.from({ length: count }, (_, index) => ({
    timestamp: Date.now(),
    text: `${text}-${index}`,
  }));

/** The batch as it currently stands, without going through serialisation. */
const batchOf = (buffer: ObserveAgentSharedBuffer) =>
  (buffer as unknown as { _mainThreadBuffer: { logs?: unknown[] } | null })
    ._mainThreadBuffer;

const pendingOf = (buffer: ObserveAgentSharedBuffer) =>
  (buffer as unknown as { _pendingLogs: unknown[] })._pendingLogs;

/**
 * Logs are the only unbounded input the agent has: snapshots are gated by
 * `maxTracesPerBatch`, but a service stuck in a retry loop produces log lines
 * with no ceiling. Left unchecked they push the payload past the shared buffer's
 * size, the whole write throws, and noisy logging silently costs the traces and
 * metrics too. These cases are about that boundary.
 */
describe("ObserveAgentSharedBuffer", () => {
  describe("empty state", () => {
    it("starts empty", () => {
      expect(createBuffer().isBufferEmpty()).toBe(true);
    });

    it("stops being empty once something is buffered", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(1));

      expect(buffer.isBufferEmpty()).toBe(false);
    });

    it("is empty again after a reset", () => {
      const buffer = createBuffer();
      buffer.pushLogs(logs(1));

      buffer.resetMainThreadBuffer();

      expect(buffer.isBufferEmpty()).toBe(true);
    });
  });

  describe("log batching", () => {
    it("keeps everything that fits in one batch", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(10));

      expect(batchOf(buffer)!.logs).toHaveLength(10);
      expect(pendingOf(buffer)).toHaveLength(0);
    });

    it("caps a batch and holds the remainder back", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + 50));

      // Deferred rather than discarded: a burst that exceeds one batch is the
      // most interesting thing the logs will ever carry.
      expect(batchOf(buffer)!.logs).toHaveLength(MAX_LOGS_PER_TRANSACTION);
      expect(pendingOf(buffer)).toHaveLength(50);
    });

    it("bounds the backlog rather than growing without limit", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + MAX_PENDING_LOGS + 500));

      // Dropping telemetry is recoverable; taking the host process down with an
      // ever-growing array is not.
      expect(batchOf(buffer)!.logs).toHaveLength(MAX_LOGS_PER_TRANSACTION);
      expect(pendingOf(buffer)).toHaveLength(MAX_PENDING_LOGS);
    });

    it("drops the newest lines when both are full", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + MAX_PENDING_LOGS + 10));

      // During a burst the opening lines usually explain it; losing the tail of
      // a retry storm costs less than losing its first cause.
      const batch = batchOf(buffer)!.logs as Array<{ text: string }>;
      expect(batch[0].text).toBe("line-0");
    });

    it("does not write to stdout when it drops", () => {
      // `pushLogs` is called from the patched `process.stdout.write`, so a log
      // line here would re-enter, drop, and log again - unbounded recursion.
      const buffer = createBuffer({ debug: true });
      const write = vi
        .spyOn(process.stdout, "write")
        .mockImplementation(() => true);

      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + MAX_PENDING_LOGS + 10));

      expect(write).not.toHaveBeenCalled();
      write.mockRestore();
    });
  });

  describe("oversized entries", () => {
    it("truncates a line past the per-entry limit", () => {
      const buffer = createBuffer();

      buffer.pushLogs([
        { timestamp: Date.now(), text: "x".repeat(MAX_LOG_ENTRY_LENGTH * 3) },
      ]);

      // One serialised payload dump can be megabytes on its own, so the count
      // cap alone does not bound the batch.
      const [entry] = batchOf(buffer)!.logs as Array<{ text: string }>;
      expect(entry.text.length).toBeLessThanOrEqual(MAX_LOG_ENTRY_LENGTH);
      expect(entry.text).toMatch(/\[truncated by observe\]$/);
    });

    it("leaves a line inside the limit untouched", () => {
      const buffer = createBuffer();
      const text = "y".repeat(100);

      buffer.pushLogs([{ timestamp: Date.now(), text }]);

      const [entry] = batchOf(buffer)!.logs as Array<{ text: string }>;
      expect(entry.text).toBe(text);
    });

    it("truncates lines held in the backlog too", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION));
      buffer.pushLogs([
        { timestamp: Date.now(), text: "z".repeat(MAX_LOG_ENTRY_LENGTH * 2) },
      ]);

      const [pending] = pendingOf(buffer) as Array<{ text: string }>;
      expect(pending.text.length).toBeLessThanOrEqual(MAX_LOG_ENTRY_LENGTH);
    });
  });

  describe("draining the backlog", () => {
    it("tops a partly-filled batch up from the backlog", () => {
      const buffer = createBuffer();
      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + 20));
      buffer.resetMainThreadBuffer();

      buffer.drainPendingLogs();

      // Without this the backlog strands itself: the process goes quiet, no new
      // `pushLogs` runs to move the lines, and the buffer reads as empty while
      // holding them until shutdown.
      expect(batchOf(buffer)!.logs).toHaveLength(20);
      expect(pendingOf(buffer)).toHaveLength(0);
    });

    it("does nothing when the backlog is empty", () => {
      const buffer = createBuffer();

      buffer.drainPendingLogs();

      expect(buffer.isBufferEmpty()).toBe(true);
    });

    it("does not overfill a batch that is already at capacity", () => {
      const buffer = createBuffer();
      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION + 100));

      buffer.drainPendingLogs();

      expect(batchOf(buffer)!.logs).toHaveLength(MAX_LOGS_PER_TRANSACTION);
      expect(pendingOf(buffer)).toHaveLength(100);
    });

    it("drains across several batches", () => {
      const buffer = createBuffer();
      buffer.pushLogs(logs(MAX_LOGS_PER_TRANSACTION * 3));

      let drained = 0;
      for (let batch = 0; batch < 2; batch++) {
        buffer.resetMainThreadBuffer();
        buffer.drainPendingLogs();
        drained += (batchOf(buffer)!.logs as unknown[]).length;
      }

      expect(drained).toBe(MAX_LOGS_PER_TRANSACTION * 2);
      expect(pendingOf(buffer)).toHaveLength(0);
    });
  });

  describe("request snapshot operation id", () => {
    const requestSnapshot = (overrides: Record<string, unknown> = {}) =>
      ({
        traceId: "trace-1",
        protocol: "http",
        traces: [],
        ...overrides,
      }) as never;

    const snapshotsOf = (buffer: ObserveAgentSharedBuffer) =>
      (batchOf(buffer) as { snapshots?: Array<{ op?: string; ou?: string }> })
        ?.snapshots;

    it("keeps the operation id when one was captured", () => {
      const buffer = createBuffer();

      buffer.insertRequestSnapshot(
        requestSnapshot({
          operationId: "/users/:id",
          attributes: { originalUrl: "/users/42" },
        }),
      );

      expect(snapshotsOf(buffer)).toEqual([
        expect.objectContaining({ op: "/users/:id" }),
      ]);
    });

    it("falls back to the request URL when no route was resolved", () => {
      const buffer = createBuffer();

      // A guard rejecting the request, or a GraphQL document that never
      // reaches a resolver, ends the trace before route metadata exists.
      buffer.insertRequestSnapshot(
        requestSnapshot({ attributes: { originalUrl: "/graphql" } }),
      );

      expect(snapshotsOf(buffer)).toEqual([
        expect.objectContaining({ op: "/graphql" }),
      ]);
    });

    it("drops a snapshot with neither, keeping the batch shippable", () => {
      const buffer = createBuffer();
      buffer.insertRequestSnapshot(
        requestSnapshot({ operationId: "/healthy" }),
      );

      // The collector rejects a whole batch over one snapshot missing its
      // operation id, so the unlabelable snapshot must never ride along.
      buffer.insertRequestSnapshot(requestSnapshot({ traceId: "trace-2" }));

      expect(snapshotsOf(buffer)).toEqual([
        expect.objectContaining({ op: "/healthy" }),
      ]);
    });
  });

  describe("job snapshot cap", () => {
    it("caps job snapshots at maxTracesPerBatch, same as request snapshots", () => {
      const buffer = createBuffer({ maxTracesPerBatch: 3 });

      for (let index = 0; index < 5; index += 1) {
        buffer.insertJobSnapshot({ name: `job-${index}` } as never);
      }

      // The buffer only empties when a flush succeeds, so without this cap a
      // stalled flush on the worker service would grow the array unboundedly.
      expect((batchOf(buffer) as { jobs?: unknown[] }).jobs).toHaveLength(3);
    });
  });

  describe("payload metadata", () => {
    it("stamps the service id on the batch", () => {
      const buffer = createBuffer();

      buffer.pushLogs(logs(1));

      expect(batchOf(buffer)).toMatchObject({ serviceId: "svc-test" });
    });

    it("reports whether log forwarding is on", () => {
      const on = createBuffer({ forwardLogs: true });
      const off = createBuffer({ forwardLogs: false });
      on.pushLogs(logs(1));
      off.pushLogs(logs(1));

      // Self-reported, because an app with forwarding on that logged nothing
      // sends no `logs` array either - presence alone cannot tell "quiet" from
      // "never configured".
      expect(batchOf(on)).toMatchObject({ forwardLogs: true });
      expect(batchOf(off)).toMatchObject({ forwardLogs: false });
    });

    it("includes the service version only when one is configured", () => {
      const versioned = createBuffer({ serviceVersion: "1.2.3" });
      const unversioned = createBuffer();
      versioned.pushLogs(logs(1));
      unversioned.pushLogs(logs(1));

      expect(batchOf(versioned)).toMatchObject({ serviceVersion: "1.2.3" });
      expect(batchOf(unversioned)).not.toHaveProperty("serviceVersion");
    });
  });

  describe("locking", () => {
    it("starts unlocked", () => {
      expect(createBuffer().isBufferLocked()).toBe(false);
    });

    it("acquires and reports the lock", () => {
      const buffer = createBuffer();

      expect(buffer.acquireLock()).not.toBe(false);
      expect(buffer.isBufferLocked()).toBe(true);
    });

    it("refuses a second acquisition while held", () => {
      const buffer = createBuffer();
      buffer.acquireLock();

      // The worker and the main thread share this buffer; two writers at once
      // would interleave bytes into an unparseable payload.
      expect(buffer.acquireLock()).toBe(false);
    });
  });

  describe("custom metrics", () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("keeps one entry for a metric whose name is cut to fit", () => {
      vi.spyOn(console, "warn").mockImplementation(() => {});
      const buffer = createBuffer();
      const counter = new Counter(`orders.${"placed.".repeat(20)}total`);

      counter.increment();
      buffer.upsertCustomMetric(counter);
      counter.increment();
      buffer.upsertCustomMetric(counter);

      // Matched against the name as sent, the second change updates the
      // first entry instead of buffering the metric twice.
      const { custom } = batchOf(buffer) as unknown as {
        custom: Array<{ n: string; v: Record<string, number> }>;
      };
      expect(custom).toHaveLength(1);
      expect(custom[0].n).toHaveLength(100);
      expect(custom[0].v).toEqual({ default: 2 });
    });
  });
});
