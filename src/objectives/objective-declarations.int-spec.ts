import {
  CallHandler,
  CanActivate,
  Controller,
  Delete,
  ExecutionContext,
  Get,
  INestApplication,
  Injectable,
  Logger,
  MiddlewareConsumer,
  Module,
  NestInterceptor,
  NestMiddleware,
  NestModule,
  Param,
  Post,
  UseGuards,
  UseInterceptors,
  VersioningType,
} from "@nestjs/common";
import { NestFactory, RouterModule } from "@nestjs/core";
import { lastValueFrom, of, tap } from "rxjs";
import type { MockInstance } from "vitest";
import { validateTelemetryPayload } from "../agent/telemetry-wire-contract.js";
import { ObserveOptions } from "../interfaces/observe-options.interface.js";
import { createObserveModule } from "../observe.module.js";
import {
  FakeCollector,
  ReceivedBatch,
  startFakeCollector,
} from "../testing/fake-collector.js";
import { Objective, ObjectiveOptions } from "./objective.decorator.js";

const observeOptions = (
  collector: FakeCollector,
  serviceId: string,
): ObserveOptions => ({
  appKey: "test-key",
  appSecret: "test-secret",
  serviceId,
  serviceVersion: "objectives-int",
  endpoint: collector.url,
  flushInterval: 1000,
  runtimeMetrics: false,
  forwardLogs: false,
});

/**
 * Sends requests to one app, each under a trace id of its own, so a test can
 * wait for exactly the batch that delivered it - which is also the batch that
 * delivered any declaration the request brought due, since both are buffered
 * in the same call.
 */
function requester(prefix: string) {
  let baseUrl = "";
  let sent = 0;
  return {
    attach(url: string) {
      baseUrl = url;
    },
    async send(
      method: string,
      path: string,
      headers: Record<string, string> = {},
    ): Promise<{ traceId: string; status: number }> {
      const traceId = `${prefix}-${++sent}`;
      const response = await fetch(`${baseUrl}${path}`, {
        method,
        headers: { "x-request-id": traceId, ...headers },
      });
      await response.arrayBuffer();
      return { traceId, status: response.status };
    },
  };
}

// Each of these is refused by the collector, so each is dropped at boot.
@Controller("refused")
class RefusedController {
  @Get("perfect")
  @Objective({ availability: 100 })
  perfect() {
    return "ok";
  }

  @Get("lax")
  @Objective({ latency: { underMs: 300, target: 89 } })
  lax() {
    return "ok";
  }

  @Get("fortnight")
  @Objective({ availability: 99.9, windowDays: 10 as never })
  fortnight() {
    return "ok";
  }

  @Get("instant")
  @Objective({ latency: { underMs: 0, target: 99 } })
  instant() {
    return "ok";
  }

  @Get("fractional")
  @Objective({ latency: { underMs: 1.5, target: 99 } })
  fractional() {
    return "ok";
  }

  @Get("empty")
  @Objective({})
  empty() {
    return "ok";
  }

  @Get("verbose")
  @Objective({ availability: 99.9, name: "n".repeat(121) })
  verbose() {
    return "ok";
  }
}

@Controller("healthy")
class HealthyController {
  @Get()
  @Objective({ availability: 99.5 })
  check() {
    return "ok";
  }

  @Get("partly")
  @Objective({ availability: 99.95 })
  @Objective({ availability: 89 })
  partly() {
    return "ok";
  }
}

@Controller({ path: "orders", version: "1" })
class OrdersController {
  @Get()
  list() {
    return [];
  }

  @Get(":id")
  @Objective({ availability: 99.9, windowDays: 7 })
  @Objective({
    latency: { underMs: 250, target: 95 },
    name: "Order lookup under 250ms",
  })
  findOne(@Param("id") id: string) {
    return { id };
  }

  @Get(":id/items")
  @Objective({ latency: { underMs: 400, target: 99 } })
  items(@Param("id") id: string) {
    return { id, items: [] };
  }

  @Delete(":id")
  @Objective({ availability: 99.5 })
  remove(@Param("id") id: string) {
    return { id, removed: true };
  }

  @Post()
  @Objective({ availability: 99.9, latency: { underMs: 800, target: 99 } })
  create() {
    throw new Error("payment provider unavailable");
  }
}

@Module({ controllers: [OrdersController] })
class AdminModule {}

@Controller("status")
class StatusController {
  @Get()
  @Objective({ availability: 99.95 })
  show() {
    return { up: true };
  }
}

@Controller()
class CatalogController {
  @Get(["catalog", "products"])
  @Objective({ availability: 99 })
  browse() {
    return [];
  }
}

@Injectable()
class StaffOnlyGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    const request = context
      .switchToHttp()
      .getRequest<{ headers: Record<string, string | undefined> }>();
    return request.headers["x-staff"] === "yes";
  }
}

@Controller("reports")
class ReportsController {
  @Get()
  @UseGuards(StaffOnlyGuard)
  @Objective({ availability: 99.9 })
  summary() {
    return { total: 0 };
  }
}

@Injectable()
class AllowGuard implements CanActivate {
  canActivate() {
    return true;
  }
}

/** Hands the handler's stream straight back: its span ends before the handler's starts. */
@Injectable()
class TimingInterceptor implements NestInterceptor {
  intercept(_context: ExecutionContext, next: CallHandler) {
    return next.handle().pipe(tap(() => undefined));
  }
}

/**
 * Awaits the handler inside its own call, the way a transaction or unit-of-work
 * interceptor does - which puts the handler's span under this one's.
 */
@Injectable()
class TransactionInterceptor implements NestInterceptor {
  async intercept(_context: ExecutionContext, next: CallHandler) {
    return of(await lastValueFrom(next.handle()));
  }
}

/** A second interceptor of the same kind. */
@Injectable()
class AuditInterceptor implements NestInterceptor {
  async intercept(_context: ExecutionContext, next: CallHandler) {
    return of(await lastValueFrom(next.handle()));
  }
}

@Controller("pipeline")
class PipelineController {
  @Get("guarded")
  @UseGuards(AllowGuard)
  @UseInterceptors(TimingInterceptor, TransactionInterceptor)
  @Objective({ availability: 99.9 })
  guarded() {
    return "ok";
  }

  @Get("audited")
  @UseInterceptors(TransactionInterceptor, AuditInterceptor)
  @Objective({ availability: 99.9 })
  audited() {
    return "ok";
  }
}

// Class middleware runs the rest of the request inside its own call, so every
// span after it - the handler's included - nests one level under it.
@Injectable()
class CorrelationMiddleware implements NestMiddleware {
  use(_request: unknown, _response: unknown, next: () => void) {
    next();
  }
}

@Injectable()
class RequestLogMiddleware implements NestMiddleware {
  use(_request: unknown, _response: unknown, next: () => void) {
    next();
  }
}

/** Behind one middleware. */
@Controller("logged")
class LoggedController {
  @Get()
  @Objective({ availability: 99.9 })
  show() {
    return "ok";
  }
}

/** Behind two - a correlation id and a request log is a common pair. */
@Controller("traced")
class TracedController {
  @Get()
  @Objective({ availability: 99.9 })
  show() {
    return "ok";
  }
}

/**
 * `@Objective` end to end, on the application shapes that decide what a
 * declaration says: a global prefix, URI versioning, a RouterModule path and
 * route parameters; guards, interceptors and middleware around the handler;
 * handlers that throw, share a path or serve several; declarations the
 * collector would refuse.
 *
 * What only this level proves is the pairing itself. The route in a
 * declaration is never written anywhere - it is taken from the handler's span
 * in a real request's trace, so it is only right if the instrumentation, the
 * adapter's route hook and the registry agree on a request Nest actually
 * served; and the result is only useful if it reaches the collector, in the
 * batch root, in a shape the API accepts.
 */
describe("@Objective: what an application declares, as the collector receives it", () => {
  let app: INestApplication;
  let collector: FakeCollector;
  let warn: MockInstance<Logger["warn"]>;
  const requests = requester("declared");

  const declarationsOf = (handler: string) =>
    collector.declarations().filter((d) => d.handler === handler);

  const objectiveWarnings = () =>
    warn.mock.calls
      .map(([message]) => String(message))
      .filter((message) => message.startsWith("@Objective on "));

  beforeAll(async () => {
    collector = await startFakeCollector();
    // Before the app exists: declarations are read, and refused ones warned
    // about, while it initializes.
    warn = vi
      .spyOn(Logger.prototype, "warn")
      .mockImplementation(() => undefined);

    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [
        ObserveModule.forRoot(observeOptions(collector, "declarations-app")),
        AdminModule,
        RouterModule.register([{ path: "admin", module: AdminModule }]),
      ],
      controllers: [
        RefusedController,
        HealthyController,
        StatusController,
        CatalogController,
        ReportsController,
        PipelineController,
        LoggedController,
        TracedController,
      ],
    })
    class DeclarationsModule implements NestModule {
      configure(consumer: MiddlewareConsumer) {
        consumer.apply(RequestLogMiddleware).forRoutes(LoggedController);
        consumer
          .apply(CorrelationMiddleware, RequestLogMiddleware)
          .forRoutes(TracedController);
      }
    }

    app = await NestFactory.create(DeclarationsModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    app.setGlobalPrefix("api");
    app.enableVersioning({ type: VersioningType.URI });
    await app.listen(0);
    requests.attach(await app.getUrl());
  });

  afterAll(async () => {
    await app?.close();
    await collector?.close();
    warn?.mockRestore();
  });

  afterEach(() => {
    // Every batch the agent sent is one the API, which validates with
    // `forbidNonWhitelisted`, would accept.
    expect(collector.violations).toEqual([]);
  });

  it("warns at boot about each objective the collector would refuse, naming the handler and why", () => {
    const refused = [
      ["RefusedController.perfect", "availability must be a percentage"],
      ["RefusedController.lax", "latency.target must be a percentage"],
      [
        "RefusedController.fortnight",
        "windowDays must be one of 7, 14, 28, 30",
      ],
      [
        "RefusedController.instant",
        "latency.underMs must be whole milliseconds",
      ],
      [
        "RefusedController.fractional",
        "latency.underMs must be whole milliseconds",
      ],
      ["RefusedController.empty", "declares neither availability nor latency"],
      ["RefusedController.verbose", "name must be at most 120 characters"],
      ["HealthyController.partly", "availability must be a percentage"],
      ["CatalogController.browse", "it serves 2 routes"],
    ];

    expect(objectiveWarnings()).toHaveLength(refused.length);
    expect(objectiveWarnings()).toEqual(
      expect.arrayContaining(
        refused.map(([handler, reason]) =>
          expect.stringContaining(`${handler} ignored: ${reason}`),
        ),
      ),
    );
  });

  it("never sends a refused declaration, while the requests' snapshots and a valid declaration still reach the collector", async () => {
    const sent = await Promise.all([
      ...[
        "perfect",
        "lax",
        "fortnight",
        "instant",
        "fractional",
        "empty",
        "verbose",
      ].map((route) => requests.send("GET", `/api/refused/${route}`)),
      requests.send("GET", "/api/healthy"),
    ]);
    expect(sent.map(({ status }) => status)).toEqual(Array(8).fill(200));

    const batches = await Promise.all(
      sent.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    // Every request was delivered, refused declaration or not...
    for (const [index, batch] of batches.entries()) {
      const snapshot = batch.snapshots!.find(
        (s) => s.ti === sent[index].traceId,
      );
      expect(snapshot?.a?.sc).toBe(200);
    }
    // ...and of their declarations only the valid one.
    expect(
      collector
        .declarations()
        .filter((d) => d.handler.startsWith("RefusedController.")),
    ).toEqual([]);
    expect(declarationsOf("HealthyController.check")).toEqual([
      {
        handler: "HealthyController.check",
        operationId: "/api/healthy",
        method: "GET",
        objectives: [{ availability: 99.5 }],
      },
    ]);
  });

  it("keeps a handler's valid objective when another on it is refused", async () => {
    const { traceId } = await requests.send("GET", "/api/healthy/partly");
    await collector.waitForTrace(traceId);

    expect(declarationsOf("HealthyController.partly")).toEqual([
      {
        handler: "HealthyController.partly",
        operationId: "/api/healthy/partly",
        method: "GET",
        objectives: [{ availability: 99.95 }],
      },
    ]);
  });

  it("sends every objective a handler declares, each exactly as given, as one declaration in the batch of the request that paired it", async () => {
    const { traceId, status } = await requests.send(
      "GET",
      "/api/v1/admin/orders/7",
    );
    expect(status).toBe(200);
    const batch = await collector.waitForTrace(traceId);

    const [declaration, ...others] = declarationsOf("OrdersController.findOne");
    expect(others).toEqual([]);
    expect(batch.objectives).toContain(declaration);
    expect(declaration).toMatchObject({
      operationId: "/api/v1/admin/orders/:id",
      method: "GET",
    });
    expect(declaration.objectives).toHaveLength(2);
    expect(declaration.objectives).toEqual(
      expect.arrayContaining([
        { availability: 99.9, windowDays: 7 },
        {
          latency: { underMs: 250, target: 95 },
          name: "Order lookup under 250ms",
        },
      ]),
    );
    expect(validateTelemetryPayload(batch, { forbidUnknown: true })).toEqual(
      [],
    );
  });

  it("names the route as served - global prefix, version, RouterModule path and parameter template - with its method", async () => {
    const { traceId, status } = await requests.send(
      "DELETE",
      "/api/v1/admin/orders/7",
    );
    expect(status).toBe(200);
    await collector.waitForTrace(traceId);

    expect(declarationsOf("OrdersController.remove")).toEqual([
      {
        handler: "OrdersController.remove",
        operationId: "/api/v1/admin/orders/:id",
        method: "DELETE",
        objectives: [{ availability: 99.5 }],
      },
    ]);
  });

  it("declares a GET route under GET alone, even once a HEAD request has reached its handler", async () => {
    // Express answers HEAD with the GET handler, and uptime probes send HEAD.
    const probe = await requests.send("HEAD", "/api/status");
    const visit = await requests.send("GET", "/api/status");
    expect([probe.status, visit.status]).toEqual([200, 200]);
    await Promise.all(
      [probe, visit].map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(
      declarationsOf("StatusController.show").map((d) => d.method),
    ).toEqual(["GET"]);
  });

  it("declares a route once, however many concrete paths reach it", async () => {
    const sent = await Promise.all(
      ["1", "2", "3"].map((id) =>
        requests.send("GET", `/api/v1/admin/orders/${id}/items`),
      ),
    );
    await Promise.all(
      sent.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(declarationsOf("OrdersController.items")).toEqual([
      {
        handler: "OrdersController.items",
        operationId: "/api/v1/admin/orders/:id/items",
        method: "GET",
        objectives: [{ latency: { underMs: 400, target: 99 } }],
      },
    ]);
  });

  it("declares nothing for a handler mounted on several paths, whichever is reached", async () => {
    // Its SLO would watch whichever path was declared last, moving with
    // every restatement - so the handler is left out, and said so at boot.
    const sent = await Promise.all([
      requests.send("GET", "/api/catalog"),
      requests.send("GET", "/api/products"),
    ]);
    expect(sent.map(({ status }) => status)).toEqual([200, 200]);
    await Promise.all(
      sent.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(declarationsOf("CatalogController.browse")).toEqual([]);
  });

  it("declares a route from a request whose handler threw", async () => {
    const { traceId, status } = await requests.send(
      "POST",
      "/api/v1/admin/orders",
    );
    expect(status).toBe(500);
    const batch = await collector.waitForTrace(traceId);

    const snapshot = batch.snapshots!.find((s) => s.ti === traceId);
    expect(snapshot?.a?.sc).toBe(500);
    expect(batch.objectives).toEqual(
      expect.arrayContaining([
        {
          handler: "OrdersController.create",
          operationId: "/api/v1/admin/orders",
          method: "POST",
          objectives: [
            { availability: 99.9, latency: { underMs: 800, target: 99 } },
          ],
        },
      ]),
    );
  });

  it("leaves a route due until a request reaches its handler: one a guard turns away declares nothing", async () => {
    const turnedAway = await requests.send("GET", "/api/reports");
    expect(turnedAway.status).toBe(403);
    const refusedBatch = await collector.waitForTrace(turnedAway.traceId);

    expect(
      refusedBatch.snapshots!.find((s) => s.ti === turnedAway.traceId)?.op,
    ).toBe("/api/reports");
    expect(declarationsOf("ReportsController.summary")).toEqual([]);

    const admitted = await requests.send("GET", "/api/reports", {
      "x-staff": "yes",
    });
    expect(admitted.status).toBe(200);
    const admittedBatch = await collector.waitForTrace(admitted.traceId);

    expect(admittedBatch.objectives).toEqual(
      expect.arrayContaining([
        {
          handler: "ReportsController.summary",
          operationId: "/api/reports",
          method: "GET",
          objectives: [{ availability: 99.9 }],
        },
      ]),
    );
  });

  it("declares a handler that runs behind a guard, interceptors or a middleware", async () => {
    const sent = await Promise.all([
      requests.send("GET", "/api/pipeline/guarded"),
      requests.send("GET", "/api/logged"),
    ]);
    await Promise.all(
      sent.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(declarationsOf("PipelineController.guarded")).toMatchObject([
      { operationId: "/api/pipeline/guarded", method: "GET" },
    ]);
    expect(declarationsOf("LoggedController.show")).toMatchObject([
      { operationId: "/api/logged", method: "GET" },
    ]);
  });

  it("declares a handler that runs behind two middlewares, or two interceptors that await it", async () => {
    // Each wrapper nests the handler's span one level deeper. Two of them are
    // an ordinary application - a correlation-id middleware and a request log,
    // a transaction interceptor and an audit one.
    const sent = await Promise.all([
      requests.send("GET", "/api/traced"),
      requests.send("GET", "/api/pipeline/audited"),
    ]);
    expect(sent.map(({ status }) => status)).toEqual([200, 200]);
    await Promise.all(
      sent.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(
      collector
        .declarations()
        .map((d) => d.handler)
        .filter((handler) =>
          ["TracedController.show", "PipelineController.audited"].includes(
            handler,
          ),
        )
        .sort(),
    ).toEqual(["PipelineController.audited", "TracedController.show"]);
  });

  it("leaves the objectives key off a batch that brings no declaration due", async () => {
    const { traceId } = await requests.send("GET", "/api/v1/admin/orders");
    const batch = await collector.waitForTrace(traceId);

    expect(batch.snapshots!.map((s) => s.ti)).toEqual([traceId]);
    expect(batch).not.toHaveProperty("objectives");
  });
});

// Typed wider than a literal, the way a shared constants file hands one
// over - so TypeScript's excess-property check never sees `owner`.
const CHECKOUT_OBJECTIVE: ObjectiveOptions & { owner: string } = {
  availability: 99.95,
  owner: "payments-team",
};

@Controller("loose")
class LooseController {
  @Get("checkout")
  @Objective(CHECKOUT_OBJECTIVE)
  checkout() {
    return "ok";
  }

  @Get("promises")
  promises() {
    return "ok";
  }
}

// Eleven latency promises on one handler, each valid on its own: under 100ms,
// under 200ms, ... under 1100ms.
const promises = Object.getOwnPropertyDescriptor(
  LooseController.prototype,
  "promises",
)!;
for (let step = 1; step <= 11; step++) {
  Objective({ latency: { underMs: step * 100, target: 99 } })(
    LooseController.prototype,
    "promises",
    promises,
  );
}

/**
 * The collector validates a batch with `forbidNonWhitelisted` and refuses it
 * whole over one bad declaration - losing every snapshot in it, and again each
 * time the declaration is restated. `objectiveProblems` exists so that never
 * happens; these are declarations that pass it and would still be refused:
 * an options object carrying a field the API does not declare, and more
 * objectives on one handler than its `ArrayMaxSize(10)` allows.
 */
describe("@Objective: declarations the collector would refuse whole", () => {
  let app: INestApplication;
  let collector: FakeCollector;
  const requests = requester("loose");

  /** The collector's `ObjectiveDeclarationDto.objectives` bound. */
  const MAX_OBJECTIVES_PER_DECLARATION = 10;

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [ObserveModule.forRoot(observeOptions(collector, "loose-app"))],
      controllers: [LooseController],
    })
    class LooseModule {}

    app = await NestFactory.create(LooseModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.listen(0);
    requests.attach(await app.getUrl());
  });

  afterAll(async () => {
    await app?.close();
    await collector?.close();
  });

  const batchOf = async (path: string): Promise<ReceivedBatch> => {
    const { traceId, status } = await requests.send("GET", path);
    expect(status).toBe(200);
    return collector.waitForTrace(traceId);
  };

  it("never sends an objective field the collector does not declare", async () => {
    const batch = await batchOf("/loose/checkout");

    expect(validateTelemetryPayload(batch, { forbidUnknown: true })).toEqual(
      [],
    );
  });

  it("never sends more objectives for one handler than the collector accepts", async () => {
    await batchOf("/loose/promises");

    for (const declaration of collector.declarations()) {
      expect(declaration.objectives.length).toBeLessThanOrEqual(
        MAX_OBJECTIVES_PER_DECLARATION,
      );
    }
  });
});

@Controller("plain")
class PlainController {
  @Get()
  list() {
    return [];
  }

  @Get(":id")
  findOne(@Param("id") id: string) {
    return { id };
  }

  @Post()
  create() {
    throw new Error("deliberate");
  }
}

/**
 * The common case: an application that declares nothing. Its batches must
 * look exactly as they did before `@Objective` existed - no `objectives` key,
 * not even an empty one.
 */
describe("@Objective: an application that declares nothing", () => {
  let app: INestApplication;
  let collector: FakeCollector;
  const requests = requester("plain");

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [ObserveModule.forRoot(observeOptions(collector, "plain-app"))],
      controllers: [PlainController],
    })
    class PlainModule {}

    app = await NestFactory.create(PlainModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.listen(0);
    requests.attach(await app.getUrl());
  });

  afterAll(async () => {
    await app?.close();
    await collector?.close();
  });

  it("sends batches with no objectives key at all", async () => {
    const first = await Promise.all([
      requests.send("GET", "/plain"),
      requests.send("GET", "/plain/1"),
      requests.send("POST", "/plain"),
    ]);
    await Promise.all(
      first.map(({ traceId }) => collector.waitForTrace(traceId)),
    );
    // A second batch, sent after the first was answered.
    const second = await requests.send("GET", "/plain/2");
    await collector.waitForTrace(second.traceId);

    expect(collector.batches.length).toBeGreaterThanOrEqual(2);
    for (const batch of collector.batches) {
      expect(batch).not.toHaveProperty("objectives");
    }
    expect(collector.violations).toEqual([]);
  });
});

@Controller("async")
class AsyncOrdersController {
  @Get("orders")
  @Objective({ availability: 99.9, latency: { underMs: 300, target: 99 } })
  list() {
    return [];
  }
}

/**
 * The registry is a provider of the module itself, so `forRootAsync` - where
 * the options exist only once a factory has run - must deliver declarations
 * exactly as `forRoot` does.
 */
describe("@Objective: registered with forRootAsync", () => {
  let app: INestApplication;
  let collector: FakeCollector;
  const requests = requester("async");

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [
        ObserveModule.forRootAsync({
          useFactory: async () => observeOptions(collector, "async-app"),
        }),
      ],
      controllers: [AsyncOrdersController],
    })
    class AsyncModule {}

    app = await NestFactory.create(AsyncModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.listen(0);
    requests.attach(await app.getUrl());
  });

  afterAll(async () => {
    await app?.close();
    await collector?.close();
  });

  it("delivers the declaration with the batch of the request that paired it", async () => {
    const { traceId, status } = await requests.send("GET", "/async/orders");
    expect(status).toBe(200);
    const batch = await collector.waitForTrace(traceId);

    expect(batch.objectives).toEqual([
      {
        handler: "AsyncOrdersController.list",
        operationId: "/async/orders",
        method: "GET",
        objectives: [
          { availability: 99.9, latency: { underMs: 300, target: 99 } },
        ],
      },
    ]);
    expect(collector.violations).toEqual([]);
  });
});
