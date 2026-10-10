import {
  BadRequestException,
  CallHandler,
  Controller,
  ExecutionContext,
  Get,
  Injectable,
  Module,
  NestInterceptor,
  Param,
  Post,
  UseInterceptors,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import {
  FastifyAdapter,
  NestFastifyApplication,
} from "@nestjs/platform-fastify";
import * as api from "@opentelemetry/api";
import { Observable } from "rxjs";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const spans = new OtelTestSpans();

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

@Injectable()
class PricingService {
  priceOf(id: string) {
    return Number(id) * 10;
  }
}

@Injectable()
class OrdersService {
  constructor(private readonly pricing: PricingService) {}

  find(id: string) {
    return { id, price: this.pricing.priceOf(id) };
  }

  fail(): never {
    throw new Error("deliberate password=hunter2");
  }
}

@Controller()
class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  @Get("orders")
  findAll() {
    return [{ id: 1 }];
  }

  @Get("orders/:id")
  findOne(@Param("id") id: string) {
    return this.orders.find(id);
  }

  @Post("orders")
  create() {
    return { created: true };
  }

  @Get("boom")
  boom() {
    return this.orders.fail();
  }

  @Get("invalid")
  invalid() {
    throw new BadRequestException("bad input");
  }
}

@Injectable()
class PassThroughInterceptor implements NestInterceptor {
  intercept(
    _context: ExecutionContext,
    next: CallHandler,
  ): Observable<unknown> {
    return next.handle();
  }
}

@Controller("intercepted")
@UseInterceptors(PassThroughInterceptor)
class InterceptedController {
  @Get()
  findAll() {
    return [{ id: 1 }];
  }
}

@Module({
  imports: [ObserveModule.forRoot({})],
  controllers: [OrdersController, InterceptedController],
  providers: [OrdersService, PricingService, PassThroughInterceptor],
})
class HttpTestModule {}

/**
 * HTTP collection on Fastify, recorded through OpenTelemetry: the HTTP and
 * Fastify snapshot suites, asserted as span trees.
 *
 * The agent never names an adapter - it relies on the three request hooks
 * every adapter is meant to implement - so the only evidence that Fastify
 * honours them is the same trees coming out of it: a SERVER root named after
 * the route template, the handler and the providers it calls nested beneath,
 * and the response status on the root, written on the error path too.
 */
describe("ObserveModule: HTTP collection on Fastify (OpenTelemetry)", () => {
  let app: NestFastifyApplication;

  beforeAll(async () => {
    installOtelGlobals();
    app = await NestFactory.create<NestFastifyApplication>(
      HttpTestModule,
      new FastifyAdapter(),
      { instrument: ObserveInstrument, logger: false },
    );
    await app.init();
    // Fastify queues its plugins and routes until `ready()`; supertest talks to
    // the raw server, which would answer 404 to everything before that.
    await app.getHttpAdapter().getInstance().ready();
  });

  afterAll(async () => {
    await app?.close();
    uninstallOtelGlobals();
  });

  beforeEach(() => spans.reset());

  /** The SERVER spans finished so far: one per request that was recorded. */
  const roots = () =>
    spans.finished.filter((span) => span.kind === api.SpanKind.SERVER);

  it("records a GET request as a SERVER span over its handler", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);

    const trace = await spans.traceOf("GET /orders");
    expect(spanTree(trace)).toBe(
      ["SERVER GET /orders", "  INTERNAL OrdersController.findAll"].join("\n"),
    );
    const root = spanNamed(trace, "GET /orders");
    expect(root.attributes).toMatchObject({
      "http.request.method": "GET",
      "http.route": "/orders",
      "url.path": "/orders",
      "http.response.status_code": 200,
      [ObserveAttributes.PROTOCOL]: "http",
      [ObserveAttributes.STATUS_CODE]: 200,
    });
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
  });

  it("times the root around the whole request", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);

    const trace = await spans.traceOf("GET /orders");
    const root = spanNamed(trace, "GET /orders");
    const handler = spanNamed(trace, "OrdersController.findAll");
    // The root ends when the response hook fires, never before the handler
    // it encloses - ending it early would cut every child off its parent in
    // a backend's waterfall.
    const nanos = ([s, ns]: api.HrTime) => s * 1e9 + ns;
    expect(nanos(root.startTime)).toBeLessThanOrEqual(nanos(handler.startTime));
    expect(nanos(root.endTime)).toBeGreaterThanOrEqual(nanos(handler.endTime));
    expect(root.duration[0]).toBeLessThan(5);
  });

  it("names the root after the route template, not the concrete path", async () => {
    // The whole point of a route: /orders/42 and /orders/99 have to aggregate
    // together, or every id becomes its own endpoint in the backend.
    await request(app.getHttpServer()).get("/orders/42").expect(200);
    await request(app.getHttpServer()).get("/orders/99").expect(200);

    await spans.waitFor((finished) =>
      finished.filter((span) => span.name === "GET /orders/:id").length >= 2
        ? true
        : undefined,
    );
    expect(roots().map((root) => root.name)).toEqual([
      "GET /orders/:id",
      "GET /orders/:id",
    ]);
    // The concrete path is still there, as `url.path`.
    expect(roots().map((root) => root.attributes["url.path"])).toEqual([
      "/orders/42",
      "/orders/99",
    ]);
    expect(roots().map((root) => root.attributes["http.route"])).toEqual([
      "/orders/:id",
      "/orders/:id",
    ]);
  });

  it("nests providers under the handler that called them", async () => {
    await request(app.getHttpServer()).get("/orders/7").expect(200);

    const trace = await spans.traceOf("GET /orders/:id");
    expect(spanTree(trace)).toBe(
      [
        "SERVER GET /orders/:id",
        "  INTERNAL OrdersController.findOne",
        "    INTERNAL OrdersService.find",
        "      INTERNAL PricingService.priceOf",
      ].join("\n"),
    );
    expect(spanNamed(trace, "PricingService.priceOf").attributes).toEqual({
      "code.function.name": "PricingService.priceOf",
    });
  });

  it("distinguishes methods on the same path", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);
    await request(app.getHttpServer()).post("/orders").expect(201);

    const trace = await spans.traceOf("POST /orders");
    expect(spanTree(trace)).toBe(
      ["SERVER POST /orders", "  INTERNAL OrdersController.create"].join("\n"),
    );
    expect(spanNamed(trace, "POST /orders").attributes).toMatchObject({
      "http.request.method": "POST",
      "http.route": "/orders",
      "http.response.status_code": 201,
    });
    await spans.traceOf("GET /orders");
    expect(roots()).toHaveLength(2);
  });

  it("fails a request whose handler threw, with the error redacted on every span", async () => {
    // A request that throws is the one most worth having, so the response
    // hook has to fire on the error path too.
    await request(app.getHttpServer()).get("/boom").expect(500);

    const trace = await spans.traceOf("GET /boom");
    expect(spanTree(trace)).toBe(
      [
        "SERVER GET /boom",
        "  INTERNAL OrdersController.boom",
        "    INTERNAL OrdersService.fail",
      ].join("\n"),
    );

    const root = spanNamed(trace, "GET /boom");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      "http.response.status_code": 500,
      "error.type": "Error",
      [ObserveAttributes.STATUS_CODE]: 500,
      [ObserveAttributes.ERROR_HANDLED]: false,
    });

    // Each step the error passed through failed with it, and carries it as
    // an `exception` event - redacted, stack included.
    for (const name of ["OrdersService.fail", "OrdersController.boom"]) {
      const step = spanNamed(trace, name);
      expect(step.status).toEqual({
        code: api.SpanStatusCode.ERROR,
        message: "deliberate password=[REDACTED]",
      });
      expect(step.events.map((event) => event.name)).toEqual(["exception"]);
      expect(step.events[0].attributes).toMatchObject({
        "exception.type": "Error",
        "exception.message": "deliberate password=[REDACTED]",
      });
    }
    expect(JSON.stringify(trace.map((span) => span.events))).not.toContain(
      "hunter2",
    );
    expect(JSON.stringify(trace.map((span) => span.status))).not.toContain(
      "hunter2",
    );
  });

  it("leaves a 4xx unset on the root: the client failed, not the server", async () => {
    // Semantic conventions keep SERVER spans' 4xx out of the error rate. The
    // handler still threw, so its own span records the exception - and the
    // root that it was raised on purpose.
    await request(app.getHttpServer()).get("/invalid").expect(400);

    const trace = await spans.traceOf("GET /invalid");
    expect(spanTree(trace)).toBe(
      ["SERVER GET /invalid", "  INTERNAL OrdersController.invalid"].join("\n"),
    );

    const root = spanNamed(trace, "GET /invalid");
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    expect(root.attributes).toMatchObject({
      "http.response.status_code": 400,
      [ObserveAttributes.STATUS_CODE]: 400,
      [ObserveAttributes.ERROR_HANDLED]: true,
    });
    expect(root.attributes["error.type"]).toBeUndefined();

    const handler = spanNamed(trace, "OrdersController.invalid");
    expect(handler.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(handler.attributes["error.type"]).toBe("BadRequestException");
  });

  it("names a request no route matched by its method alone", async () => {
    // No route was triggered, so there is no template to name it by - and the
    // raw path would give every probe for a missing URL its own span name.
    await request(app.getHttpServer()).get("/nowhere/123").expect(404);

    const trace = await spans.traceOf("GET");
    expect(spanTree(trace)).toBe("SERVER GET");
    const root = spanNamed(trace, "GET");
    expect(root.attributes["http.route"]).toBeUndefined();
    expect(root.attributes).toMatchObject({
      "url.path": "/nowhere/123",
      "http.response.status_code": 404,
    });
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
  });

  it("continues the trace a caller sent in its traceparent header", async () => {
    const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    await request(app.getHttpServer())
      .get("/orders")
      .set("traceparent", `00-${traceId}-00f067aa0ba902b7-01`)
      .expect(200);

    const trace = await spans.traceOf("GET /orders");
    const root = spanNamed(trace, "GET /orders");
    expect(root.spanContext().traceId).toBe(traceId);
    expect(root.parentSpanContext?.spanId).toBe("00f067aa0ba902b7");
    expect(spanTree(trace)).toBe(
      ["SERVER GET /orders", "  INTERNAL OrdersController.findAll"].join("\n"),
    );
  });

  it("never puts request headers on a span, failed request or not", async () => {
    // The snapshot recorder attaches an allow-list of headers to a failed
    // request; the OTel recorder attaches none, so a credential cannot leak
    // through either path.
    await request(app.getHttpServer())
      .get("/boom")
      .set("authorization", "Bearer should-never-leave")
      .expect(500);
    await request(app.getHttpServer())
      .get("/orders")
      .set("authorization", "Bearer should-never-leave")
      .expect(200);

    await spans.traceOf("GET /boom");
    await spans.traceOf("GET /orders");
    const recorded = JSON.stringify(
      spans.finished.map((span) => [span.attributes, span.events]),
    );
    expect(recorded).not.toContain("should-never-leave");
  });

  it("records one root, in its own trace, per request", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);
    await request(app.getHttpServer()).get("/orders").expect(200);
    await request(app.getHttpServer()).get("/orders").expect(200);

    // Poll until the third arrives, then assert nothing extra did - a
    // double-registered hook would show up here and nowhere else.
    await spans.waitFor(() => (roots().length >= 3 ? true : undefined));
    expect(roots()).toHaveLength(3);
    expect(
      new Set(roots().map((root) => root.spanContext().traceId)).size,
    ).toBe(3);
    expect(spans.finished).toHaveLength(6);
  });

  /**
   * Nest binds the stage after an interceptor to the async context in which
   * `next.handle()` was called (`AsyncResource.bind` in its interceptors
   * consumer), so the handler inherits the interceptor's span as its caller -
   * a span a pass-through interceptor (the shape of `@sentry/nestjs`'s) has
   * already ended by the time the handler starts.
   *
   * The snapshot recorder files such a handler at the root. The OTel recorder
   * sees the ended interceptor span as the parent, finds it no longer
   * recording, and records the handler - and everything under it - not at
   * all: `OtelSpanRecorder.runStep` returns `fn(false)` when
   * `currentSpan()` is not recording. The handler belongs under the nearest
   * span still open, the root.
   */
  it.fails(
    "still records the handler behind an interceptor whose span already ended",
    async () => {
      await request(app.getHttpServer()).get("/intercepted").expect(200);

      const trace = await spans.traceOf("GET /intercepted");
      expect(spanTree(trace)).toBe(
        [
          "SERVER GET /intercepted",
          "  INTERNAL PassThroughInterceptor.intercept",
          "  INTERNAL InterceptedController.findAll",
        ].join("\n"),
      );
    },
  );
});
