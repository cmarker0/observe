import type * as Otel from "@opentelemetry/api";
import {
  constants,
  monitorEventLoopDelay,
  type NodeGCPerformanceDetail,
  performance,
  type PerformanceEntry,
  PerformanceObserver,
} from "node:perf_hooks";
import * as v8 from "node:v8";
import { Counter } from "../custom-metrics/counter.js";
import { Gauge } from "../custom-metrics/gauge.js";
import { Summary } from "../custom-metrics/summary.js";
import type { MeterSource } from "../interfaces/observe-options.interface.js";
import type { OpenTelemetryApi } from "../recorder/otel-span-recorder.js";

export const METER_NAME = "@nestjs/observe";

const DEFAULT_LABEL = "default";

/** Event-loop delay is sampled this often, in ms, as `instrumentation-runtime-node` does. */
const EVENT_LOOP_DELAY_RESOLUTION = 10;

/** `v8js.gc.duration` buckets, in seconds, as semantic conventions advise. */
const GC_DURATION_BUCKETS = [0.01, 0.1, 1, 10];

/**
 * V8's young-generation mark-sweep, which Node names only from 24.20 - see
 * `NodeRuntimeMetricsService` for the same constant.
 */
const NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP =
  (
    constants as typeof constants & {
      NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP?: number;
    }
  ).NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP ?? 2;

/** `v8js.gc.type` for a `gc` entry's `detail.kind`. */
function gcTypeOf(kind: number | undefined): string | undefined {
  switch (kind) {
    case constants.NODE_PERFORMANCE_GC_MINOR:
    case NODE_PERFORMANCE_GC_MINOR_MARK_SWEEP:
      return "minor";
    case constants.NODE_PERFORMANCE_GC_MAJOR:
      return "major";
    case constants.NODE_PERFORMANCE_GC_INCREMENTAL:
      return "incremental";
    case constants.NODE_PERFORMANCE_GC_WEAKCB:
      return "weakcb";
    default:
      return undefined;
  }
}

type CustomMetric = Counter<any> | Gauge<any> | Summary<any>;

/**
 * Metrics through the OpenTelemetry Meter API, for `createObserveModule({
 * opentelemetry })`. Readings go to whatever meter provider the application
 * registered, on its reader's schedule; nothing goes to the collector.
 *
 * - Custom metrics: a counter is an asynchronous counter and a gauge an
 *   asynchronous gauge, both read off the metric's own cumulative values; a
 *   summary is a histogram, recorded per observation. Labels and tags become
 *   attributes.
 * - Runtime metrics use the names, units and attributes of the semantic
 *   conventions - the same ones `@opentelemetry/instrumentation-runtime-node`
 *   reports, so either one feeds the same dashboards. Run one of the two.
 */
export class OtelMetrics {
  private readonly meter: Otel.Meter;
  private readonly registered = new WeakSet<CustomMetric>();

  constructor(
    private readonly api: OpenTelemetryApi,
    meterSource?: MeterSource,
  ) {
    // As with the tracer: the global provider is a proxy until the SDK
    // registers, so a meter taken here still reaches one started later.
    this.meter = (meterSource ?? api.metrics).getMeter(
      METER_NAME,
    ) as Otel.Meter;
  }

  /** Reports `metric` through the meter. Idempotent per metric. */
  register(metric: CustomMetric): void {
    if (this.registered.has(metric)) {
      return;
    }
    this.registered.add(metric);
    const options = { description: metric.description };

    if (metric instanceof Summary) {
      const histogram = this.meter.createHistogram(metric.name, options);
      metric["_onObserve"] = (value: number, label: string | undefined) => {
        histogram.record(value, {
          ...metric.tags,
          ...(label !== undefined && { label }),
        });
      };
      return;
    }

    const instrument =
      metric instanceof Counter
        ? this.meter.createObservableCounter(metric.name, options)
        : this.meter.createObservableGauge(metric.name, options);
    instrument.addCallback((result) => {
      for (const [key, value] of Object.entries(metric.value)) {
        result.observe(value, { ...metric.tags, ...attributesOf(key) });
      }
    });
  }

  /**
   * Starts reporting runtime metrics. Returns what stops it.
   *
   * Values are read when the application's reader collects. Event-loop delay
   * and utilisation cover the time since the previous collection, so a
   * second reader on the same provider splits those windows between them.
   */
  startRuntime(): () => void {
    const { meter } = this;
    const { ValueType } = this.api;

    const delay = monitorEventLoopDelay({
      resolution: EVENT_LOOP_DELAY_RESOLUTION,
    });
    delay.enable();
    const delayGauges = {
      min: () => delay.min,
      max: () => delay.max,
      mean: () => delay.mean,
      stddev: () => delay.stddev,
      p50: () => delay.percentile(50),
      p90: () => delay.percentile(90),
      p99: () => delay.percentile(99),
    };
    const delayInstruments = Object.keys(delayGauges).map((stat) =>
      meter.createObservableGauge(`nodejs.eventloop.delay.${stat}`, {
        description: `Event loop ${stat} delay.`,
        unit: "s",
      }),
    );

    const utilization = meter.createObservableGauge(
      "nodejs.eventloop.utilization",
      { description: "Event loop utilization.", unit: "1" },
    );
    const loopTime = meter.createObservableCounter("nodejs.eventloop.time", {
      description:
        "Cumulative duration of time the event loop has been in each state.",
      unit: "s",
    });
    const heapLimit = meter.createObservableUpDownCounter(
      "v8js.memory.heap.limit",
      {
        description: "Total heap memory size pre-allocated.",
        unit: "By",
        valueType: ValueType.INT,
      },
    );
    const heapUsed = meter.createObservableUpDownCounter(
      "v8js.memory.heap.used",
      {
        description: "Heap memory size allocated.",
        unit: "By",
        valueType: ValueType.INT,
      },
    );
    const spaceAvailable = meter.createObservableUpDownCounter(
      "v8js.heap.space.available_size",
      {
        description: "Heap space available size.",
        unit: "By",
        valueType: ValueType.INT,
      },
    );
    const spacePhysical = meter.createObservableUpDownCounter(
      "v8js.heap.space.physical_size",
      {
        description: "Committed size of a heap space.",
        unit: "By",
        valueType: ValueType.INT,
      },
    );
    const cpuTime = meter.createObservableCounter("process.cpu.time", {
      description: "Total CPU seconds broken down by different CPU modes.",
      unit: "s",
    });
    const memoryUsage = meter.createObservableUpDownCounter(
      "process.memory.usage",
      {
        description: "The amount of physical memory in use.",
        unit: "By",
        valueType: ValueType.INT,
      },
    );

    let lastUtilization = performance.eventLoopUtilization();
    const observeRuntime: Otel.BatchObservableCallback = (result) => {
      // `monitorEventLoopDelay` reports nanoseconds, and NaN before its
      // first sample - a window with no samples reports nothing.
      if (delay.count > 0) {
        Object.values(delayGauges).forEach((read, index) =>
          result.observe(delayInstruments[index], read() / 1e9),
        );
      }
      delay.reset();

      const current = performance.eventLoopUtilization();
      result.observe(
        utilization,
        performance.eventLoopUtilization(current, lastUtilization).utilization,
      );
      lastUtilization = current;
      result.observe(loopTime, current.active / 1000, {
        "nodejs.eventloop.state": "active",
      });
      result.observe(loopTime, current.idle / 1000, {
        "nodejs.eventloop.state": "idle",
      });

      for (const space of v8.getHeapSpaceStatistics()) {
        const attributes = { "v8js.heap.space.name": space.space_name };
        result.observe(heapLimit, space.space_size, attributes);
        result.observe(heapUsed, space.space_used_size, attributes);
        result.observe(spaceAvailable, space.space_available_size, attributes);
        result.observe(spacePhysical, space.physical_space_size, attributes);
      }

      const cpu = process.cpuUsage();
      result.observe(cpuTime, cpu.user / 1e6, { "cpu.mode": "user" });
      result.observe(cpuTime, cpu.system / 1e6, { "cpu.mode": "system" });
      result.observe(memoryUsage, process.memoryUsage.rss());
    };
    const observables = [
      ...delayInstruments,
      utilization,
      loopTime,
      heapLimit,
      heapUsed,
      spaceAvailable,
      spacePhysical,
      cpuTime,
      memoryUsage,
    ];
    meter.addBatchObservableCallback(observeRuntime, observables);

    const gcDuration = meter.createHistogram("v8js.gc.duration", {
      description: "Garbage collection duration.",
      unit: "s",
      advice: { explicitBucketBoundaries: GC_DURATION_BUCKETS },
    });
    const gcObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        recordGc(gcDuration, entry);
      }
    });
    gcObserver.observe({ type: "gc" });

    return () => {
      meter.removeBatchObservableCallback(observeRuntime, observables);
      gcObserver.disconnect();
      delay.disable();
    };
  }
}

function recordGc(histogram: Otel.Histogram, entry: PerformanceEntry): void {
  // Typed only on marks and measures, but every `gc` entry carries one.
  const { detail } = entry as PerformanceEntry & {
    detail?: NodeGCPerformanceDetail | null;
  };
  const type = gcTypeOf(detail?.kind);
  histogram.record(
    entry.duration / 1000,
    type === undefined ? {} : { "v8js.gc.type": type },
  );
}

/**
 * A custom metric's series key as attributes: the default series has none,
 * a labelled one is the label object `stringifyLabel` serialised.
 */
function attributesOf(key: string): Otel.Attributes {
  if (key === DEFAULT_LABEL) {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(key);
    return typeof parsed === "object" && parsed !== null
      ? (parsed as Otel.Attributes)
      : {};
  } catch {
    return {};
  }
}
