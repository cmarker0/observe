import { Controller, Get, Injectable, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import * as api from "@opentelemetry/api";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { createRequire } from "node:module";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  parentIdOf,
  spanIdOf,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";
import { TracerService } from "../services/tracer.service.js";

const spans = new OtelTestSpans();

/**
 * Enabled before the app exists. It patches `http` the next time anything
 * requires it - Express has already, so the require below is what applies
 * the patch to the module every server and client shares.
 */
const instrumentation = new HttpInstrumentation();
instrumentation.setTracerProvider(spans.provider);
instrumentation.enable();
const http = createRequire(import.meta.url)(
  "node:http",
) as typeof import("node:http");

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

/** Where the app listens, for a handler that calls back into it. */
let baseUrl = "";

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
    return this.orders.list()[0];
  }

  @Get("trace-id")
  traceId() {
    return { traceId: this.tracer.currentTraceId() };
  }

  /** A downstream call through `node:http`, which only the SDK records. */
  @Get("chain")
  chain() {
    return new Promise((resolve, reject) => {
      http
        .get(`${baseUrl}/orders/1`, (response) => {
          let body = "";
          response.on("data", (chunk) => (body += chunk));
          response.on("end", () => resolve(JSON.parse(body)));
        })
        .on("error", reject);
    });
  }
}

@Module({
  imports: [
    // The combination the docs recommend beside the SDK's own HTTP
    // instrumentation: ours for the Nest structure, theirs for the wire.
    ObserveModule.forRoot({ outgoing: { http: false, database: false } }),
  ],
  controllers: [OrdersController],
  providers: [OrdersService],
})
class CoexistenceModule {}

/**
 * Observe beside `@opentelemetry/instrumentation-http`, the way the
 * Kubernetes Operator's Node auto-instrumentation runs it: the SDK opens the
 * SERVER span for the inbound request, so the operation must nest under it as
 * INTERNAL rather than repeat it, and the SDK's client spans must still land
 * under the provider that made the call.
 */
describe("ObserveModule: beside instrumentation-http", () => {
  let app: NestExpressApplication;

  /**
   * `trace` without the test's own request: supertest goes through the
   * patched `http` too, so the SDK records it as the trace's CLIENT root.
   */
  const fromServer = (trace: ReadableSpan[]) =>
    trace.filter(
      (span) =>
        span.kind !== api.SpanKind.CLIENT || parentIdOf(span) !== undefined,
    );

  const serverSpans = () =>
    spans.finished.filter((span) => span.kind === api.SpanKind.SERVER);

  beforeAll(async () => {
    installOtelGlobals();
    app = await NestFactory.create<NestExpressApplication>(CoexistenceModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.listen(0, "127.0.0.1");
    baseUrl = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
    instrumentation.disable();
    uninstallOtelGlobals();
  });

  beforeEach(() => spans.reset());

  it("nests the operation under the SDK's SERVER span instead of repeating it", async () => {
    await request(app.getHttpServer()).get("/orders/7").expect(200);

    const trace = fromServer(await spans.traceOf("GET /orders/:id"));
    expect(spanTree(trace)).toBe(
      [
        "SERVER GET",
        "  INTERNAL GET /orders/:id",
        "    INTERNAL OrdersController.findOne",
        "      INTERNAL OrdersService.list",
      ].join("\n"),
    );
    expect(serverSpans()).toHaveLength(1);

    const operation = spanNamed(trace, "GET /orders/:id");
    expect(operation.attributes).toMatchObject({
      "http.request.method": "GET",
      "http.route": "/orders/:id",
      "http.response.status_code": 200,
    });
  });

  it("leaves extraction to the SDK, and correlates on the trace it continued", async () => {
    const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    const { body } = await request(app.getHttpServer())
      .get("/trace-id")
      .set("traceparent", `00-${traceId}-00f067aa0ba902b7-01`)
      .expect(200);

    const trace = await spans.traceOf("GET /trace-id");
    const server = spanNamed(trace, "GET");
    const operation = spanNamed(trace, "GET /trace-id");
    expect(server.spanContext().traceId).toBe(traceId);
    expect(parentIdOf(server)).toBe("00f067aa0ba902b7");
    // Under the SDK's span, not beside it as a second child of the caller.
    expect(parentIdOf(operation)).toBe(spanIdOf(server));
    expect(body).toEqual({ traceId });
  });

  it("puts the SDK's client span under the handler that made the call", async () => {
    await request(app.getHttpServer()).get("/chain").expect(200);

    const trace = fromServer(await spans.traceOf("GET /chain"));
    // One trace, two SERVER spans - one per inbound request - and the call
    // between them recorded once, by the SDK, under the calling handler.
    expect(spanTree(trace)).toBe(
      [
        "SERVER GET",
        "  INTERNAL GET /chain",
        "    INTERNAL OrdersController.chain",
        "      CLIENT GET",
        "        SERVER GET",
        "          INTERNAL GET /orders/:id",
        "            INTERNAL OrdersController.findOne",
        "              INTERNAL OrdersService.list",
      ].join("\n"),
    );
  });
});
