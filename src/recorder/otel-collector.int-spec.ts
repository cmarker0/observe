import { Controller, Get, Injectable, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  BasicTracerProvider,
  BatchSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { ChildProcess, spawn } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer, Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { TracerService } from "../services/tracer.service.js";
import {
  installOtelGlobals,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

/**
 * An OpenTelemetry Collector binary - the core distribution is enough. The
 * suite is skipped without one, so it runs where someone asked for it:
 *
 *   OTELCOL_BIN=/path/to/otelcol npx vitest run -c vitest.int.config.ts \
 *     src/recorder/otel-collector.int-spec.ts
 */
const collectorBin = process.env.OTELCOL_BIN;
const haveCollector = !!collectorBin && existsSync(collectorBin);

const resource = resourceFromAttributes({ "service.name": "observe-e2e" });

/** Where the collector is told to listen, chosen before anything exports. */
const port = haveCollector ? await freePort() : 0;
const endpoint = `http://127.0.0.1:${port}`;

const tracerProvider = new BasicTracerProvider({
  resource,
  spanProcessors: [
    new BatchSpanProcessor(
      new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }),
      { scheduledDelayMillis: 100 },
    ),
  ],
});
const meterProvider = new MeterProvider({
  resource,
  readers: [
    new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter({ url: `${endpoint}/v1/metrics` }),
      // Flushed by hand below; the schedule must not race it.
      exportIntervalMillis: 60_000,
    }),
  ],
});

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider, meterProvider },
});

@Injectable()
class OrdersService {
  list() {
    return [{ id: 1 }];
  }
}

@Controller()
class OrdersController {
  constructor(
    private readonly orders: OrdersService,
    private readonly tracer: TracerService,
  ) {}

  @Get("orders/:id")
  findOne() {
    this.tracer.counter("orders_read").increment();
    return this.orders.list()[0];
  }
}

@Module({
  imports: [ObserveModule.forRoot({ runtimeMetrics: true })],
  controllers: [OrdersController],
  providers: [OrdersService],
})
class CollectorModule {}

/** An OTLP/JSON span as the collector's file exporter writes it. */
interface OtlpSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind: number;
  attributes?: Array<{ key: string; value: Record<string, unknown> }>;
}

/**
 * The whole path, with nothing in memory standing in for it: a Nest app
 * exporting over OTLP/HTTP with the batch processor and a periodic metric
 * reader, a real Collector receiving it, and the Collector's own file
 * exporter as what the assertions read. What reaches the file is what any
 * backend behind a Collector would get.
 */
describe.skipIf(!haveCollector)(
  "ObserveModule: through a real OTel Collector",
  () => {
    let collector: ChildProcess;
    let app: NestExpressApplication;
    let dir: string;
    let collectorLog = "";

    const readLines = (file: string): unknown[] =>
      existsSync(file)
        ? readFileSync(file, "utf8")
            .split("\n")
            .filter(Boolean)
            .map((line) => JSON.parse(line) as unknown)
        : [];

    async function waitFor<T>(read: () => T | undefined, what: string) {
      const deadline = Date.now() + 10_000;
      for (;;) {
        const value = read();
        if (value !== undefined) {
          return value;
        }
        if (Date.now() > deadline) {
          throw new Error(
            `Timed out waiting for ${what}. Collector:\n${collectorLog}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    const spansIn = (file: string): OtlpSpan[] =>
      (
        readLines(file) as Array<{
          resourceSpans: Array<{
            resource: { attributes: OtlpSpan["attributes"] };
            scopeSpans: Array<{ scope: { name: string }; spans: OtlpSpan[] }>;
          }>;
        }>
      ).flatMap((line) =>
        line.resourceSpans.flatMap((resourceSpans) =>
          resourceSpans.scopeSpans.flatMap((scope) => scope.spans),
        ),
      );

    const attribute = (span: OtlpSpan, key: string) =>
      span.attributes?.find((entry) => entry.key === key)?.value;

    beforeAll(async () => {
      dir = mkdtempSync(join(tmpdir(), "observe-otelcol-"));
      writeFileSync(
        join(dir, "config.yaml"),
        [
          "receivers:",
          "  otlp:",
          "    protocols:",
          "      http:",
          `        endpoint: 127.0.0.1:${port}`,
          "exporters:",
          "  file/traces:",
          `    path: ${join(dir, "traces.jsonl")}`,
          "  file/metrics:",
          `    path: ${join(dir, "metrics.jsonl")}`,
          "service:",
          "  telemetry:",
          "    metrics:",
          "      level: none",
          "  pipelines:",
          "    traces:",
          "      receivers: [otlp]",
          "      exporters: [file/traces]",
          "    metrics:",
          "      receivers: [otlp]",
          "      exporters: [file/metrics]",
        ].join("\n"),
      );
      collector = spawn(collectorBin!, ["--config", join(dir, "config.yaml")], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      collector.stdout?.on("data", (chunk) => (collectorLog += chunk));
      collector.stderr?.on("data", (chunk) => (collectorLog += chunk));
      await waitFor(
        () => (collectorLog.includes("Everything is ready") ? true : undefined),
        "the collector to start",
      );

      installOtelGlobals();
      app = await NestFactory.create<NestExpressApplication>(CollectorModule, {
        instrument: ObserveInstrument,
        logger: false,
      });
      await app.init();
    });

    afterAll(async () => {
      await app?.close();
      await tracerProvider?.shutdown();
      await meterProvider?.shutdown();
      uninstallOtelGlobals();
      collector?.kill("SIGTERM");
    rmSync(dir, { recursive: true, force: true });
    });

    it("delivers the request's span tree, continuing the caller's trace", async () => {
      const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
      await request(app.getHttpServer())
        .get("/orders/7")
        .set("traceparent", `00-${traceId}-00f067aa0ba902b7-01`)
        .expect(200);
      await tracerProvider.forceFlush();

      const spans = await waitFor(() => {
        const found = spansIn(join(dir, "traces.jsonl")).filter(
          (span) => span.traceId === traceId,
        );
        // The root ends last, after the response is written.
        return found.some((span) => span.name === "GET /orders/:id")
          ? found
          : undefined;
      }, "the request's spans");

      expect(spans.map((span) => span.name).sort()).toEqual([
        "GET /orders/:id",
        "OrdersController.findOne",
        "OrdersService.list",
      ]);
      const byName = new Map(spans.map((span) => [span.name, span]));
      const root = byName.get("GET /orders/:id")!;
      const handler = byName.get("OrdersController.findOne")!;
      const service = byName.get("OrdersService.list")!;
      expect(root.parentSpanId).toBe("00f067aa0ba902b7");
      expect(handler.parentSpanId).toBe(root.spanId);
      expect(service.parentSpanId).toBe(handler.spanId);
      // OTLP's SPAN_KIND_SERVER.
      expect(root.kind).toBe(2);
      expect(attribute(root, "http.route")).toEqual({
        stringValue: "/orders/:id",
      });
      expect(attribute(root, "http.response.status_code")).toEqual({
        intValue: "200",
      });

      const [line] = readLines(join(dir, "traces.jsonl")) as Array<{
        resourceSpans: Array<{
          resource: { attributes: OtlpSpan["attributes"] };
          scopeSpans: Array<{ scope: { name: string } }>;
        }>;
      }>;
      expect(line.resourceSpans[0].resource.attributes).toContainEqual({
        key: "service.name",
        value: { stringValue: "observe-e2e" },
      });
      expect(line.resourceSpans[0].scopeSpans[0].scope.name).toBe(
        "@nestjs/observe",
      );
    });

    it("delivers custom and runtime metrics under their names", async () => {
      await request(app.getHttpServer()).get("/orders/1").expect(200);
      await meterProvider.forceFlush();

      const names = await waitFor(() => {
        const found = new Set(
          (
            readLines(join(dir, "metrics.jsonl")) as Array<{
              resourceMetrics: Array<{
                scopeMetrics: Array<{ metrics: Array<{ name: string }> }>;
              }>;
            }>
          ).flatMap((line) =>
            line.resourceMetrics.flatMap((resourceMetrics) =>
              resourceMetrics.scopeMetrics.flatMap((scope) =>
                scope.metrics.map((metric) => metric.name),
              ),
            ),
          ),
        );
        return found.has("orders_read") ? found : undefined;
      }, "the metrics");

      expect(names).toContain("nodejs.eventloop.utilization");
      expect(names).toContain("v8js.memory.heap.used");
      expect(names).toContain("process.cpu.time");
    });
  },
);

/** A port nothing is listening on, for the collector to take. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer((socket: Socket) => socket.destroy());
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        typeof address === "object" && address
          ? resolve(address.port)
          : reject(new Error("no port")),
      );
    });
  });
}
