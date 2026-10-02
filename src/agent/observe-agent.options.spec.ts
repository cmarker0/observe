import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { ObserveAgentWorker } from "./observe-agent.worker.js";

/**
 * Covers option resolution, not the agent.
 *
 * `runtimeMetrics` gauges how loaded a process is; `profiling` samples stacks
 * to say which code it was running. They are different signals with different
 * costs, and the cases below are mostly about keeping them independent - the
 * cheap one must not switch on the one that samples the hot path.
 *
 * `profiling` is currently shelved - commented out of `ObserveOptions`, and
 * `startContinuousProfiling()` is no longer called - so the cases that mention
 * it assert that it stays off. The implementation is kept and tested
 * elsewhere; these guard the wiring for the day it comes back.
 *
 * `runtimeMetrics` was once called `profiler`, one letter from `profiling` for
 * an unrelated signal. The alias has since been removed outright.
 */
describe("ObserveAgentWorker options", () => {
  const collect = vi.fn();
  const sharedBuffer = { addNodeRuntimeMetrics: vi.fn() };
  const metrics = { collectNodeRuntimeMetrics: collect };

  let worker: ObserveAgentWorker;

  const build = (options: Partial<ObserveModuleOptionsWithDefaults>) => {
    worker = new ObserveAgentWorker(
      sharedBuffer as never,
      { serviceId: "svc", ...options } as ObserveModuleOptionsWithDefaults,
      metrics as never,
    );
    // The worker thread and the profiler are out of scope here; only the
    // option resolution is under test.
    vi.spyOn(worker, "initializeWorker").mockImplementation(() => undefined);
    return worker;
  };

  const runtimeMetricsStarted = () =>
    Boolean(
      (worker as unknown as { runtimeMetricsInterval: unknown })
        .runtimeMetricsInterval,
    );

  afterEach(async () => {
    await worker?.onApplicationShutdown();
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  describe("flushInterval", () => {
    // The flush itself touches the shared buffer, which is a stub here; only
    // the cadence the timer was armed with is under test.
    const armFlush = (options: Partial<ObserveModuleOptionsWithDefaults>) => {
      vi.useFakeTimers();
      build(options);
      const flush = vi
        .spyOn(worker, "flush")
        .mockImplementation(() => undefined);
      const warn = vi
        .spyOn(
          (worker as unknown as { logger: { warn: (m: string) => void } })
            .logger,
          "warn",
        )
        .mockImplementation(() => undefined);
      worker.onModuleInit();
      return { flush, warn };
    };

    it("flushes at the minimum cadence when asked for less", () => {
      // Used to warn that the interval had been raised to 1s and then arm the
      // timer with the original value anyway - `flushInterval: 50` flushed
      // twenty times a second while claiming otherwise.
      const { flush, warn } = armFlush({ flushInterval: 50 });

      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("Setting to minimum of 1000ms"),
      );

      vi.advanceTimersByTime(999);
      expect(flush).not.toHaveBeenCalled();

      vi.advanceTimersByTime(1);
      expect(flush).toHaveBeenCalledTimes(1);

      vi.advanceTimersByTime(1000);
      expect(flush).toHaveBeenCalledTimes(2);
    });

    it("keeps an interval at or above the minimum as given", () => {
      const { flush, warn } = armFlush({ flushInterval: 2500 });

      expect(warn).not.toHaveBeenCalled();

      vi.advanceTimersByTime(2499);
      expect(flush).not.toHaveBeenCalled();

      vi.advanceTimersByTime(1);
      expect(flush).toHaveBeenCalledTimes(1);
    });

    it("defaults to five seconds when the option is not set", () => {
      const { flush, warn } = armFlush({});

      expect(warn).not.toHaveBeenCalled();

      vi.advanceTimersByTime(4999);
      expect(flush).not.toHaveBeenCalled();

      vi.advanceTimersByTime(1);
      expect(flush).toHaveBeenCalledTimes(1);
    });
  });

  it("collects runtime metrics when asked by the new name", () => {
    build({ runtimeMetrics: true }).onModuleInit();

    expect(runtimeMetricsStarted()).toBe(true);
  });

  it("does not collect when explicitly switched off", () => {
    build({ runtimeMetrics: false }).onModuleInit();

    expect(runtimeMetricsStarted()).toBe(false);
  });

  it("treats an unset option as off, leaving the default to the metrics service", () => {
    // Not what an application sees: by the time the worker reads the option,
    // NodeRuntimeMetricsService - which it is injected with - has defaulted it
    // to true in its own constructor. The stub here does not, which leaves the
    // worker's own reading under test, so the default lives in one place.
    build({}).onModuleInit();

    expect(runtimeMetricsStarted()).toBe(false);
  });

  it("does not start the sampling profiler alongside runtime metrics", () => {
    // The whole reason for the rename: these are different signals with
    // different costs, and enabling the cheap one must not enable the one that
    // samples the hot path.
    build({ runtimeMetrics: true }).onModuleInit();

    expect(
      (worker as unknown as { cpuProfiler: unknown }).cpuProfiler,
    ).toBeNull();
  });

  // `profiling` is shelved: the option is commented out of ObserveOptions and
  // `startContinuousProfiling()` is no longer called from `onModuleInit`. The
  // implementation stays, so the independence this spec is about still has to
  // hold the day it is restored - hence the cast rather than deleting the case.
  // If passing it ever *does* start something, that is the regression.
  it("does not start runtime metrics alongside the sampling profiler", () => {
    build({
      profiling: { enabled: true },
    } as Partial<ObserveModuleOptionsWithDefaults>).onModuleInit();

    expect(runtimeMetricsStarted()).toBe(false);
  });

  it("leaves the shelved profiler switched off even when asked for it", () => {
    build({
      profiling: { enabled: true },
    } as Partial<ObserveModuleOptionsWithDefaults>).onModuleInit();

    expect(
      (worker as unknown as { cpuProfiler: unknown }).cpuProfiler,
    ).toBeNull();
  });
});
