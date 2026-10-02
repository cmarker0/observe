import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from "@nestjs/common";
import * as os from "node:os";
import {
  constants,
  monitorEventLoopDelay,
  type NodeGCPerformanceDetail,
  performance,
  type PerformanceEntry,
  PerformanceObserver,
} from "node:perf_hooks";
import { NodeRuntimeMetrics } from "../interfaces/node-runtime-metrics.interface.js";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { OBSERVE_OPTIONS } from "../observe.constants.js";

type GcBreakdown = NonNullable<NodeRuntimeMetrics["gc"]["breakdown"]>;

/**
 * V8's young-generation mark-sweep (`kGCTypeMinorMarkSweep`, `1 << 1`), which
 * replaces the scavenger under `--minor-ms`. V8 reports it with the same value
 * on every supported release, but Node only names it from 24.20 and
 * `@types/node` not at all.
 */
const NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP =
  (
    constants as typeof constants & {
      NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP?: number;
    }
  ).NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP ?? 2;

/**
 * The breakdown bucket for a `gc` entry's `detail.kind`. A weak-callback pass
 * (`NODE_PERFORMANCE_GC_WEAKCB`) has none: it is the embedder's finalisers
 * running after a collection, not a collection of either generation, so it
 * counts towards the window's totals and nothing more.
 */
function gcBucketOf(kind: number | undefined): keyof GcBreakdown | undefined {
  switch (kind) {
    case constants.NODE_PERFORMANCE_GC_MINOR:
    case NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP:
      return "minor";
    case constants.NODE_PERFORMANCE_GC_MAJOR:
      return "major";
    case constants.NODE_PERFORMANCE_GC_INCREMENTAL:
      return "incremental";
    default:
      return undefined;
  }
}

@Injectable()
export class NodeRuntimeMetricsService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly logger = new Logger(NodeRuntimeMetricsService.name);
  private eventLoopDelayMonitor: ReturnType<
    typeof monitorEventLoopDelay
  > | null = null;
  private gcObserver: PerformanceObserver | null = null;
  private gcCount = 0;
  private gcTotalDuration = 0;
  private gcBreakdown: GcBreakdown | null = null;
  // Seeded here rather than in `onModuleInit`, which never runs when runtime
  // metrics are disabled and returns early before setting them. A sample taken
  // in that window used to difference against `null`, which reads as the epoch
  // and pins CPU usage at ~0%.
  private lastCpuUsage: NodeJS.CpuUsage = process.cpuUsage();
  private lastCpuUsageTimestamp: number = Date.now();
  private lastEventLoopUtilization: ReturnType<
    typeof performance.eventLoopUtilization
  > = performance.eventLoopUtilization();
  private osTotalMemory: number = os.totalmem();

  constructor(
    @Inject(OBSERVE_OPTIONS)
    private readonly options: ObserveModuleOptionsWithDefaults,
  ) {
    this.options.runtimeMetrics ??= true;
  }

  onModuleInit() {
    if (!this.options.runtimeMetrics) {
      return;
    }

    this.lastCpuUsage = process.cpuUsage();
    this.lastCpuUsageTimestamp = Date.now();
    this.lastEventLoopUtilization = performance.eventLoopUtilization();
    this.osTotalMemory = os.totalmem();

    this.monitorEventLoopDelay();
    this.observeGcPerformance();
  }

  async onApplicationShutdown() {
    if (this.gcObserver) {
      this.gcObserver.disconnect();
      this.logger.debug("Garbage collection observer disconnected.");
    }

    if (this.eventLoopDelayMonitor) {
      this.eventLoopDelayMonitor.disable();
      this.logger.debug("Event loop delay monitoring disabled.");
    }
  }

  monitorEventLoopDelay() {
    if (this.options.debug) {
      this.logger.debug("Monitoring event loop delay.");
    }
    const h = monitorEventLoopDelay();
    h.enable();

    this.eventLoopDelayMonitor = h;
  }

  observeGcPerformance() {
    if (this.options.debug) {
      this.logger.debug("Observing garbage collection performance.");
    }

    this.gcObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        this.recordGarbageCollection(entry);
      }
    });

    this.gcObserver.observe({ type: "gc", buffered: true });
  }

  private recordGarbageCollection(entry: PerformanceEntry) {
    if (entry.entryType !== "gc") {
      return;
    }

    this.gcCount++;
    this.gcTotalDuration += entry.duration;

    // Typed only on marks and measures, but every `gc` entry carries one.
    const { detail } = entry as PerformanceEntry & {
      detail?: NodeGCPerformanceDetail | null;
    };
    const kind = gcBucketOf(detail?.kind);
    if (!kind) {
      return;
    }

    // Seeded with all three kinds at zero on the first collection of the
    // window: "no major collections happened" and "major collections were
    // not reported" are different facts, and only the seeding keeps them
    // apart downstream.
    this.gcBreakdown ??= {
      minor: { count: 0, duration: 0 },
      major: { count: 0, duration: 0 },
      incremental: { count: 0, duration: 0 },
    };
    const bucket = (this.gcBreakdown[kind] ??= {
      count: 0,
      duration: 0,
    });
    bucket.count++;
    bucket.duration += entry.duration;
  }

  collectNodeRuntimeMetrics(): NodeRuntimeMetrics {
    // Nothing was started, so there is nothing to collect: with metrics turned
    // off the histogram is never enabled, and every number below would be
    // fabricated. This used to surface as a TypeError from dereferencing the
    // absent monitor; it is stated outright so the reason survives.
    if (!this.eventLoopDelayMonitor) {
      throw new Error(
        "Runtime metrics are not being collected. `collectNodeRuntimeMetrics()` requires `onModuleInit()` to have run with the `runtimeMetrics` option enabled.",
      );
    }

    const timestamp = Date.now();
    const elapsedTime = timestamp - this.lastCpuUsageTimestamp;
    const memoryUsage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();
    const diffCpuUsage = process.cpuUsage(this.lastCpuUsage);
    // Differenced against the previous sample's reading, like CPU: read bare,
    // utilisation is cumulative since the process started, and a long-running
    // process averages any busy stretch into a flat line.
    const eventLoopUtilization = performance.eventLoopUtilization();
    const eventLoop = performance.eventLoopUtilization(
      eventLoopUtilization,
      this.lastEventLoopUtilization,
    );
    // `mean` is NaN until the histogram has recorded a sample since it was
    // enabled or last reset, so a flush that lands immediately after startup -
    // or right after the previous one - would otherwise ship NaN into a numeric
    // column, where it survives, poisons every average built over it and makes
    // the runtime alert comparisons undefined.
    const meanDelay = this.eventLoopDelayMonitor.mean;
    const lag = Number.isFinite(meanDelay) ? meanDelay / 1e6 : 0; // ns → ms

    const metrics: NodeRuntimeMetrics = {
      memory: {
        rss: memoryUsage.rss / 1024 / 1024, // Convert bytes to MB
        heapTotal: memoryUsage.heapTotal / 1024 / 1024, // Convert bytes to MB
        heapUsed: memoryUsage.heapUsed / 1024 / 1024, // Convert bytes to MB
        external: memoryUsage.external / 1024 / 1024, // Convert bytes to MB
        arrayBuffers: memoryUsage.arrayBuffers / 1024 / 1024, // Convert bytes to MB
        percentageUsed: (memoryUsage.rss / this.osTotalMemory) * 100,
      },
      cpu: {
        user: diffCpuUsage.user / 1000, // Convert from µs to ms
        system: diffCpuUsage.system / 1000, // Convert from µs to ms
        percentageUsed:
          ((diffCpuUsage.user + diffCpuUsage.system) / 1000 / elapsedTime) *
          100,
      },
      eventLoop: {
        lag,
        utilization: eventLoop.utilization,
      },
      gc: {
        count: this.gcCount,
        totalDuration: this.gcTotalDuration,
        breakdown: { ...this.gcBreakdown },
      },
    };

    this.resetGcMetrics();
    // Left alone, the histogram's `mean` covers every delay since startup, and
    // a long-running process flattens any burst of blocking into it. Resetting
    // also forgets when the histogram last ticked, so a stall that starts
    // before its next tick - one in the same timers pass as this collection,
    // say - is recorded in neither window.
    this.eventLoopDelayMonitor.reset();
    this.lastEventLoopUtilization = eventLoopUtilization;
    this.updateLastCpuUsage(cpuUsage, timestamp);

    return metrics;
  }

  private resetGcMetrics() {
    this.gcCount = 0;
    this.gcTotalDuration = 0;
    this.gcBreakdown = null;
  }

  private updateLastCpuUsage(cpuUsage: NodeJS.CpuUsage, timestamp: number) {
    this.lastCpuUsage = cpuUsage;
    this.lastCpuUsageTimestamp = timestamp;
  }
}
