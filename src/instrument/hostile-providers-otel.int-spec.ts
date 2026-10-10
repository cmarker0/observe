import { Controller, Get, Inject, Injectable, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import * as api from "@opentelemetry/api";
import { CLS_REQ, ClsModule, ClsService } from "nestjs-cls";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
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

/**
 * A provider that refuses every property read outside Nest's own lifecycle
 * hooks - the allowlist the strict CLS proxies keep, minus everything else
 * they let through, so the instance decorator gets no help from it.
 */
const HOSTILE_PROXY = Symbol("HOSTILE_PROXY");
const LIFECYCLE_HOOKS = new Set<PropertyKey>([
  "onModuleInit",
  "onApplicationBootstrap",
  "onModuleDestroy",
  "beforeApplicationShutdown",
  "onApplicationShutdown",
]);
const hostileProxy = new Proxy(
  {},
  {
    get(_target, property) {
      if (LIFECYCLE_HOOKS.has(property)) {
        return undefined;
      }
      throw new Error(`hostile proxy: no reading ${String(property)}`);
    },
  },
);

/** A provider whose accessor throws when inspected rather than called. */
@Injectable()
class ThrowingGetterService {
  get config(): never {
    throw new Error("config is not loaded yet");
  }

  ping() {
    return "pong";
  }
}

@Injectable()
class RequestInfoService {
  // The strict CLS_REQ proxy itself, resolved per request through the CLS
  // context the middleware opened.
  constructor(@Inject(CLS_REQ) private readonly req: { url: string }) {}

  path() {
    return this.req.url;
  }
}

@Controller()
class StatusController {
  constructor(
    private readonly cls: ClsService,
    private readonly info: RequestInfoService,
    private readonly throwing: ThrowingGetterService,
  ) {}

  @Get("status")
  status() {
    return {
      ok: true,
      hasRequestId: typeof this.cls.getId() === "string",
      path: this.info.path(),
      ping: this.throwing.ping(),
    };
  }
}

@Module({
  imports: [
    // Registers the CLS_REQ / CLS_RES proxy providers in strict mode - their
    // `get` trap throws ProxyProviderNotResolvedException for any property
    // not on a small allowlist whenever no CLS context is active, and
    // bootstrap always runs outside one.
    ClsModule.forRoot({
      global: true,
      middleware: { mount: true, generateId: true },
    }),
    ObserveModule.forRoot({}),
  ],
  controllers: [StatusController],
  providers: [
    RequestInfoService,
    ThrowingGetterService,
    { provide: HOSTILE_PROXY, useValue: hostileProxy },
  ],
})
class ClsTestModule {}

/**
 * nestjs/nest#17553 again, with spans recorded through OpenTelemetry: the
 * container hands *every* provider - the strict CLS proxies, a proxy that
 * refuses all reads, a class whose getter throws - to the instance decorator,
 * whose structural inspection used to read properties on them and crash the
 * whole bootstrap. Skipping what cannot be inspected must cost nothing but
 * those providers' own spans: requests still come out as span trees.
 */
describe("ObserveModule: bootstrap alongside hostile providers (OpenTelemetry)", () => {
  let app: NestExpressApplication;

  beforeAll(() => installOtelGlobals());

  afterAll(async () => {
    await app?.close();
    uninstallOtelGlobals();
  });

  beforeEach(() => spans.reset());

  it("boots with the strict CLS proxy providers registered", async () => {
    // Nest wraps the instance decorator in a safety net of its own that
    // swallows a throw with a warning, so booting proves little by itself:
    // the warning not being logged is what shows the module coped.
    const warnings: string[] = [];
    const quiet = () => undefined;
    app = await NestFactory.create<NestExpressApplication>(ClsTestModule, {
      instrument: ObserveInstrument,
      logger: {
        log: quiet,
        error: quiet,
        debug: quiet,
        verbose: quiet,
        warn: (message: unknown) => {
          warnings.push(String(message));
        },
      },
    });
    await app.init();

    expect(
      warnings.filter((line) => line.includes("instanceDecorator")),
    ).toEqual([]);
  });

  it("hands every hostile provider back from the instance decorator untouched", () => {
    // The CLS_REQ proxy as the container holds it - strict, and outside any
    // CLS context here - and the hand-rolled ones.
    const decorate = ObserveInstrument!.instanceDecorator;
    const clsRequest: unknown = app.get(CLS_REQ);
    for (const hostile of [clsRequest, app.get(HOSTILE_PROXY)]) {
      expect(decorate(hostile)).toBe(hostile);
    }
    // An inspectable class is still instrumented; inspecting it must not
    // trip its throwing getter.
    const throwing = new ThrowingGetterService();
    expect(() => decorate(throwing)).not.toThrow();
  });

  it("serves requests with a working CLS context, as a SERVER span", async () => {
    // Not just alive: the CLS middleware still does its job, and the request
    // is still recorded, with the middleware that opened the context under it.
    await request(app.getHttpServer()).get("/status").expect(200, {
      ok: true,
      hasRequestId: true,
      path: "/status",
      ping: "pong",
    });

    const trace = await spans.traceOf("GET /status");
    const root = spanNamed(trace, "GET /status");
    expect(root.kind).toBe(api.SpanKind.SERVER);
    expect(root.attributes).toMatchObject({
      "http.request.method": "GET",
      "http.route": "/status",
      "http.response.status_code": 200,
    });
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    expect(spanTree(trace)).toMatch(
      /^SERVER GET \/status\n {2}INTERNAL ClsMiddleware\.use\n/,
    );
    // Nothing failed on the way: inspecting the hostile providers threw
    // nowhere a span could see it.
    expect(trace.map((span) => span.status.code)).not.toContain(
      api.SpanStatusCode.ERROR,
    );
  });

  /**
   * `ClsMiddleware.use` is instrumented, and its step span ends when `use`
   * settles - but `next()`, called inside it, carries its async context on to
   * the guards, interceptors and handler that run afterwards. They find the
   * middleware's ended span as their parent; the recorder walks up to the
   * nearest span still open - the request's root - rather than drop them, as
   * it once did, recording no controller or provider span at all.
   */
  it("records the handler and the providers it calls beneath the request", async () => {
    await request(app.getHttpServer()).get("/status").expect(200);

    const trace = await spans.traceOf("GET /status");
    expect(spanTree(trace)).toContain(
      [
        "  INTERNAL StatusController.status",
        "    INTERNAL ClsService.getId",
        "    INTERNAL RequestInfoService.path",
        "    INTERNAL ThrowingGetterService.ping",
      ].join("\n"),
    );
    expect(
      spanNamed(trace, "StatusController.status").parentSpanContext?.spanId,
    ).toBe(spanNamed(trace, "GET /status").spanContext().spanId);
  });
});
