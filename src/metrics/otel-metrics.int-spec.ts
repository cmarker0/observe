import { ConsoleLogger, Controller, Get, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import * as api from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  MeterProvider,
  MetricData,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import request from "supertest";
import type { MockInstance } from "vitest";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import { createObserveModule } from "../observe.module.js";
import { TracerService } from "../services/tracer.service.js";
import { testObserveOptions } from "../testing/observe-harness.js";

class PullReader extends MetricReader {
  protected onForceFlush() {
    return Promise.resolve();
  }
  protected onShutdown() {
    return Promise.resolve();
  }
}

const reader = new PullReader();
const meterProvider = new MeterProvider({ readers: [reader] });
const spans = new InMemorySpanExporter();
const tracerProvider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(spans)],
});

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider, meterProvider },
});

@Controller()
class OrdersController {
  private readonly logger = new ConsoleLogger("Orders", { json: true });

  constructor(private readonly tracer: TracerService) {}

  @Get("orders")
  list() {
    this.tracer
      .counter<"status">("orders_listed", { labels: ["status"] })
      .increment({ status: "ok" });
    this.logger.log("listing");
    return [];
  }
}

@Module({
  imports: [
    ObserveModule.forRoot(testObserveOptions({ runtimeMetrics: true })),
  ],
  controllers: [OrdersController],
})
class MetricsTestModule {}

/**
 * Metrics and log correlation with `opentelemetry` on: custom and runtime
 * metrics reach the application's meter provider instead of the collector,
 * and a JSON log line names the span it was written in.
 */
describe("ObserveModule: OpenTelemetry metrics and logs", () => {
  let app: NestExpressApplication;
  let upsertCustomMetric: MockInstance;
  let addNodeRuntimeMetrics: MockInstance;

  async function collect(): Promise<Map<string, MetricData>> {
    const { resourceMetrics } = await reader.collect();
    return new Map(
      resourceMetrics.scopeMetrics
        .flatMap((scope) => scope.metrics)
        .map((metric) => [metric.descriptor.name, metric]),
    );
  }

  beforeAll(async () => {
    api.context.setGlobalContextManager(
      new AsyncLocalStorageContextManager().enable(),
    );
    app = await NestFactory.create<NestExpressApplication>(MetricsTestModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    const buffer = app.get(ObserveAgentSharedBuffer);
    upsertCustomMetric = vi.spyOn(buffer, "upsertCustomMetric");
    addNodeRuntimeMetrics = vi.spyOn(buffer, "addNodeRuntimeMetrics");
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    await meterProvider.shutdown();
    api.context.disable();
  });

  it("reports custom metrics through the meter, not the collector", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);

    const counter = (await collect()).get("orders_listed");
    expect(counter?.dataPoints.map((point) => point.attributes)).toEqual([
      { status: "ok" },
    ]);
    expect(upsertCustomMetric).not.toHaveBeenCalled();
  });

  it("reports runtime metrics through the meter, not the collector", async () => {
    const reported = await collect();
    expect(reported.has("nodejs.eventloop.utilization")).toBe(true);
    expect(reported.has("v8js.memory.heap.used")).toBe(true);
    expect(addNodeRuntimeMetrics).not.toHaveBeenCalled();
  });

  it("names the trace and span a JSON log line was written in", async () => {
    let logged = "";
    const write = vi
      .spyOn(process.stdout, "write")
      .mockImplementation((chunk: string | Uint8Array) => {
        logged += String(chunk);
        return true;
      });
    try {
      await request(app.getHttpServer()).get("/orders").expect(200);
    } finally {
      write.mockRestore();
    }

    const line = JSON.parse(logged.trim()) as Record<string, unknown>;
    const handler = spans
      .getFinishedSpans()
      .findLast((span) => span.name === "OrdersController.list")!;
    expect(line).toMatchObject({
      message: "listing",
      traceId: handler.spanContext().traceId,
      spanId: handler.spanContext().spanId,
    });
  });
});
