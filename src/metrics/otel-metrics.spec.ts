import * as api from "@opentelemetry/api";
import {
  DataPoint,
  Histogram,
  MeterProvider,
  MetricData,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { Counter } from "../custom-metrics/counter.js";
import { Gauge } from "../custom-metrics/gauge.js";
import { Summary } from "../custom-metrics/summary.js";
import { OtelMetrics } from "./otel-metrics.js";

/** Collects on demand, so a test reads exactly what one collection sees. */
class PullReader extends MetricReader {
  protected onForceFlush() {
    return Promise.resolve();
  }
  protected onShutdown() {
    return Promise.resolve();
  }
}

describe("OtelMetrics", () => {
  let reader: PullReader;
  let provider: MeterProvider;
  let metrics: OtelMetrics;

  beforeEach(() => {
    reader = new PullReader();
    provider = new MeterProvider({ readers: [reader] });
    metrics = new OtelMetrics(api, provider);
  });

  afterEach(async () => {
    await provider.shutdown();
  });

  async function collect(): Promise<Map<string, MetricData>> {
    const { resourceMetrics } = await reader.collect();
    const byName = new Map<string, MetricData>();
    for (const scope of resourceMetrics.scopeMetrics) {
      for (const metric of scope.metrics) {
        byName.set(metric.descriptor.name, metric);
      }
    }
    return byName;
  }

  const pointsOf = (metric: MetricData | undefined) =>
    (metric?.dataPoints ?? []) as Array<DataPoint<unknown>>;

  describe("custom metrics", () => {
    it("reports a counter's cumulative series, labels and tags as attributes", async () => {
      const counter = new Counter<"route">("logins", "Logins", ["route"]);
      counter.tags = { team: "auth" };
      metrics.register(counter);
      counter.increment({ route: "/login" });
      counter.increment({ route: "/login" }, 2);
      counter.increment({ route: "/sso" });

      const reported = (await collect()).get("logins");
      expect(reported?.descriptor.description).toBe("Logins");
      expect(
        pointsOf(reported).map((point) => [point.attributes, point.value]),
      ).toEqual([
        [{ team: "auth", route: "/login" }, 3],
        [{ team: "auth", route: "/sso" }, 1],
      ]);
    });

    it("reports a gauge's current value", async () => {
      const gauge = new Gauge("queue_depth");
      metrics.register(gauge);
      gauge.setValue(7);

      expect(
        pointsOf((await collect()).get("queue_depth")).map((p) => p.value),
      ).toEqual([7]);
      gauge.setValue(4);
      expect(
        pointsOf((await collect()).get("queue_depth")).map((p) => p.value),
      ).toEqual([4]);
    });

    it("records each summary observation into a histogram", async () => {
      const summary = new Summary<"fast" | "slow">("latency", {
        labels: ["fast", "slow"],
      });
      metrics.register(summary);
      summary.observe(10, "fast");
      summary.observe(30, "fast");
      summary.observe(500, "slow");

      const points = pointsOf((await collect()).get("latency")) as Array<
        DataPoint<Histogram>
      >;
      expect(
        points.map((point) => [
          point.attributes,
          point.value.count,
          point.value.sum,
        ]),
      ).toEqual([
        [{ label: "fast" }, 2, 40],
        [{ label: "slow" }, 1, 500],
      ]);
    });

    it("registers a metric once however often it is handed over", async () => {
      const counter = new Counter("jobs");
      metrics.register(counter);
      metrics.register(counter);
      counter.increment();

      expect(pointsOf((await collect()).get("jobs"))).toHaveLength(1);
    });
  });

  describe("runtime metrics", () => {
    let stop: (() => void) | undefined;

    afterEach(() => {
      stop?.();
      stop = undefined;
    });

    it("reports the runtime under semantic-convention names and units", async () => {
      stop = metrics.startRuntime();
      // Long enough for the delay histogram to take samples.
      await new Promise((resolve) => setTimeout(resolve, 50));
      const reported = await collect();

      expect(reported.get("nodejs.eventloop.delay.p99")?.descriptor.unit).toBe(
        "s",
      );
      const p99 = pointsOf(reported.get("nodejs.eventloop.delay.p99"))[0];
      // Seconds, not nanoseconds: an idle loop waits about the resolution.
      expect(p99.value).toBeGreaterThan(0);
      expect(p99.value).toBeLessThan(1);

      const utilization = pointsOf(
        reported.get("nodejs.eventloop.utilization"),
      );
      expect(utilization[0].value).toBeGreaterThanOrEqual(0);
      expect(utilization[0].value).toBeLessThanOrEqual(1);

      expect(
        pointsOf(reported.get("nodejs.eventloop.time")).map(
          (point) => point.attributes["nodejs.eventloop.state"],
        ),
      ).toEqual(["active", "idle"]);
      expect(
        pointsOf(reported.get("process.cpu.time")).map(
          (point) => point.attributes["cpu.mode"],
        ),
      ).toEqual(["user", "system"]);

      const heap = pointsOf(reported.get("v8js.memory.heap.used"));
      expect(
        heap.map((point) => point.attributes["v8js.heap.space.name"]),
      ).toContain("old_space");
      expect(reported.get("v8js.memory.heap.used")?.descriptor.unit).toBe("By");
      expect(pointsOf(reported.get("process.memory.usage"))[0].value).toEqual(
        expect.any(Number),
      );
    });

    it("records garbage collections by type, in seconds", async () => {
      stop = metrics.startRuntime();
      setFlagsFromString("--expose-gc");
      const gc = runInNewContext("gc") as () => void;
      gc();
      // `gc` entries are delivered on a later tick.
      await new Promise((resolve) => setTimeout(resolve, 20));

      const points = pointsOf(
        (await collect()).get("v8js.gc.duration"),
      ) as Array<DataPoint<Histogram>>;
      const major = points.find(
        (point) => point.attributes["v8js.gc.type"] === "major",
      );
      expect(major?.value.count).toBeGreaterThanOrEqual(1);
      expect(major!.value.sum).toBeLessThan(10);
    });

    it("stops reporting once stopped", async () => {
      metrics.startRuntime()();
      expect(
        pointsOf((await collect()).get("nodejs.eventloop.utilization")),
      ).toHaveLength(0);
    });
  });
});
