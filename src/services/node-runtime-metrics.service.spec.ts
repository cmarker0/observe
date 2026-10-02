import { constants, type PerformanceEntry } from "node:perf_hooks";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { NodeRuntimeMetricsService } from "./node-runtime-metrics.service.js";

/**
 * Samples the process every flush. The interesting behaviour is not the numbers
 * themselves - they come from Node - but the bookkeeping around them: every
 * figure describes one window, so CPU and event loop utilisation are *deltas*
 * against the previous sample, the delay histogram and GC counters must reset
 * after being read, and the monitors have to be torn down or they outlive the
 * application.
 */
describe("NodeRuntimeMetricsService", () => {
  const build = (options: Partial<ObserveModuleOptionsWithDefaults> = {}) =>
    new NodeRuntimeMetricsService({
      ...options,
    } as ObserveModuleOptionsWithDefaults);
  const sleep = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));
  // Holds the event loop the way a synchronous request handler would.
  const block = (ms: number) => {
    const until = Date.now() + ms;
    while (Date.now() < until) {
      /* burn */
    }
  };

  let service: NodeRuntimeMetricsService;

  afterEach(async () => {
    // Both the event-loop histogram and the GC observer keep the process alive.
    await service?.onApplicationShutdown();
  });

  describe("collection", () => {
    beforeEach(() => {
      service = build();
      service.onModuleInit();
    });

    it("reports memory in megabytes with a percentage of the host", () => {
      const metrics = service.collectNodeRuntimeMetrics();

      // Bytes would make every dashboard axis unreadable; the conversion is the
      // only reason these are not raw `process.memoryUsage()` values.
      expect(metrics.memory.rss).toBeGreaterThan(0);
      expect(metrics.memory.rss).toBeLessThan(100_000);
      expect(metrics.memory.heapUsed).toBeLessThanOrEqual(
        metrics.memory.heapTotal,
      );
      expect(metrics.memory.percentageUsed).toBeGreaterThan(0);
      expect(metrics.memory.percentageUsed).toBeLessThan(100);
    });

    it("reports CPU as the work done since the previous sample", () => {
      service.collectNodeRuntimeMetrics();

      // Busy-wait so the second window has measurable CPU in it.
      block(20);
      const second = service.collectNodeRuntimeMetrics();

      // A cumulative reading would grow forever and make "CPU used this minute"
      // meaningless.
      expect(second.cpu.user).toBeGreaterThanOrEqual(0);
      expect(second.cpu.system).toBeGreaterThanOrEqual(0);
      expect(second.cpu.percentageUsed).toBeGreaterThanOrEqual(0);
    });

    it("does not let CPU usage grow without bound across samples", () => {
      const first = service.collectNodeRuntimeMetrics();
      const second = service.collectNodeRuntimeMetrics();

      // Two samples taken back to back cover almost no time, so the second must
      // not report the whole process lifetime again.
      expect(second.cpu.user).toBeLessThanOrEqual(first.cpu.user + 1000);
    });

    it("reports a finite lag on the very first sample", () => {
      // The histogram's `mean` is NaN until it has recorded something, and a
      // flush can land immediately after startup. NaN survives into a numeric
      // column, poisons every average built over it, and makes the runtime
      // alert comparisons undefined.
      const metrics = service.collectNodeRuntimeMetrics();

      expect(Number.isFinite(metrics.eventLoop.lag)).toBe(true);
      expect(metrics.eventLoop.lag).toBeGreaterThanOrEqual(0);
    });

    it("reports event loop lag in milliseconds and a utilisation ratio", async () => {
      // Give the histogram a moment to collect a sample so the conversion is
      // actually exercised.
      await sleep(30);
      const metrics = service.collectNodeRuntimeMetrics();

      // The histogram is in nanoseconds; shipping those as "lag" would read as a
      // million-millisecond stall.
      expect(metrics.eventLoop.lag).toBeGreaterThanOrEqual(0);
      expect(metrics.eventLoop.lag).toBeLessThan(60_000);
      expect(metrics.eventLoop.utilization).toBeGreaterThanOrEqual(0);
      expect(metrics.eventLoop.utilization).toBeLessThanOrEqual(1);
    });

    it("does not carry delay samples over into the next window", async () => {
      await sleep(100);
      const first = service.collectNodeRuntimeMetrics();
      const second = service.collectNodeRuntimeMetrics();

      // The histogram cannot tick between two synchronous reads, so the second
      // window holds no samples at all. Carried over, the first window's would
      // be reported again - and in a long-running process every window would
      // be the mean since startup, which nothing moves.
      expect(first.eventLoop.lag).toBeGreaterThan(0);
      expect(second.eventLoop.lag).toBe(0);
    });

    it("reports a stall at full weight in the window it happened in", async () => {
      // A quiet stretch first, standing in for a process that has been up for
      // a while: averaged together with it, the stall would barely register.
      await sleep(300);
      const quiet = service.collectNodeRuntimeMetrics();

      // The histogram has to tick once after the reset before it can measure
      // anything; a stall that starts sooner is recorded in neither window.
      await sleep(20);
      block(200);
      await sleep(25);
      const stalled = service.collectNodeRuntimeMetrics();

      expect(stalled.eventLoop.lag).toBeGreaterThan(quiet.eventLoop.lag * 2);
    });

    it("reports utilisation for the window rather than since startup", async () => {
      // The baseline is taken when monitoring starts, so this window is nearly
      // all work...
      block(100);
      const busy = service.collectNodeRuntimeMetrics();
      // ...and this one nearly all waiting. Read cumulatively, both would be
      // the process lifetime's ratio, and a busy minute in a long-running
      // process would not show.
      await sleep(100);
      const idle = service.collectNodeRuntimeMetrics();

      expect(busy.eventLoop.utilization).toBeGreaterThan(0.5);
      expect(idle.eventLoop.utilization).toBeLessThan(0.5);
    });

    it("reports a GC section on every sample", () => {
      const metrics = service.collectNodeRuntimeMetrics();

      expect(metrics.gc.count).toEqual(expect.any(Number));
      expect(metrics.gc.totalDuration).toEqual(expect.any(Number));
      expect(metrics.gc).toHaveProperty("breakdown");
    });

    it("resets the GC counters after they are read", () => {
      // Provoke a collection so there is something to reset.
      if (global.gc) {
        global.gc();
      }
      service.collectNodeRuntimeMetrics();
      const second = service.collectNodeRuntimeMetrics();

      // Counters describe one flush window; carrying them forward would make
      // every window look worse than the last.
      expect(second.gc.count).toBe(0);
      expect(second.gc.totalDuration).toBe(0);
    });

    it("returns a fresh object each time rather than mutating one", () => {
      const first = service.collectNodeRuntimeMetrics();
      const second = service.collectNodeRuntimeMetrics();

      // The encoder reads these after the fact; a shared object would have been
      // overwritten by the next sample before it was serialised.
      expect(first).not.toBe(second);
      expect(first.gc).not.toBe(second.gc);
    });
  });

  describe("garbage collection breakdown", () => {
    // A collection of a given kind cannot be provoked on demand, so each window
    // is fed the entries Node would deliver. It is opened and read
    // synchronously, and the real observer only delivers on a later tick, so
    // nothing else can land in it.
    const windowOf = (...entries: { kind: number; duration: number }[]) => {
      service.collectNodeRuntimeMetrics();
      for (const { kind, duration } of entries) {
        (
          service as unknown as {
            recordGarbageCollection(entry: PerformanceEntry): void;
          }
        ).recordGarbageCollection({
          entryType: "gc",
          duration,
          detail: { kind },
        } as unknown as PerformanceEntry);
      }
      return service.collectNodeRuntimeMetrics().gc;
    };

    beforeEach(() => {
      service = build();
      service.onModuleInit();
    });

    it("buckets each collection by the kind Node reports", () => {
      const gc = windowOf(
        { kind: constants.NODE_PERFORMANCE_GC_MINOR, duration: 1 },
        { kind: constants.NODE_PERFORMANCE_GC_MAJOR, duration: 10 },
        { kind: constants.NODE_PERFORMANCE_GC_INCREMENTAL, duration: 100 },
      );

      // Scavenges used to be reported as major collections, major collections
      // as incremental marking, and incremental marking not at all.
      expect(gc.breakdown).toEqual({
        minor: { count: 1, duration: 1 },
        major: { count: 1, duration: 10 },
        incremental: { count: 1, duration: 100 },
      });
    });

    it("counts a young-generation mark-sweep as a minor collection", () => {
      // V8's `kGCTypeMinorMarkSweep`, which replaces the scavenger under
      // `--minor-ms`. Spelled out because Node only names it from 24.20.
      const gc = windowOf({ kind: 2, duration: 5 });

      expect(gc.breakdown?.minor).toEqual({ count: 1, duration: 5 });
    });

    it("counts weak-callback passes towards the totals but no bucket", () => {
      const gc = windowOf(
        { kind: constants.NODE_PERFORMANCE_GC_WEAKCB, duration: 3 },
        { kind: constants.NODE_PERFORMANCE_GC_MINOR, duration: 1 },
      );

      // Finalisers running after a collection are GC time on the main thread,
      // but not a collection of either generation.
      expect(gc.count).toBe(2);
      expect(gc.totalDuration).toBe(4);
      expect(gc.breakdown).toEqual({
        minor: { count: 1, duration: 1 },
        major: { count: 0, duration: 0 },
        incremental: { count: 0, duration: 0 },
      });
    });

    it("starts every window's breakdown empty", () => {
      windowOf({ kind: constants.NODE_PERFORMANCE_GC_MAJOR, duration: 10 });

      // Read straight after, so no collection can have landed in between.
      expect(service.collectNodeRuntimeMetrics().gc.breakdown).toEqual({});
    });
  });

  describe("runtime metrics disabled", () => {
    it("does not start any monitor", () => {
      service = build({ runtimeMetrics: false });

      service.onModuleInit();

      // Nothing was started, so nothing needs collecting - and the histogram in
      // particular would otherwise hold the event loop open for a user who
      // explicitly turned profiling off.
      expect(() => service.collectNodeRuntimeMetrics()).toThrow();
    });

    it("shuts down cleanly having started nothing", async () => {
      service = build({ runtimeMetrics: false });
      service.onModuleInit();

      await expect(service.onApplicationShutdown()).resolves.toBeUndefined();
    });
  });

  describe("defaults", () => {
    it("collects unless told otherwise", () => {
      service = build();
      service.onModuleInit();

      expect(() => service.collectNodeRuntimeMetrics()).not.toThrow();
    });

    it("leaves an explicit false alone", () => {
      const options = {
        runtimeMetrics: false,
      } as ObserveModuleOptionsWithDefaults;

      new NodeRuntimeMetricsService(options);

      // `??=` must not overwrite a deliberate opt-out.
      expect(options.runtimeMetrics).toBe(false);
      service = build();
    });
  });

  describe("shutdown", () => {
    it("can be called twice", async () => {
      service = build();
      service.onModuleInit();

      await service.onApplicationShutdown();
      await expect(service.onApplicationShutdown()).resolves.toBeUndefined();
    });
  });
});
