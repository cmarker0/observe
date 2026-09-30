import {
  Controller,
  Get,
  INestApplication,
  Module,
  Type,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import { validateTelemetryPayload } from "../agent/telemetry-wire-contract.js";
import { ObserveOptions } from "../interfaces/observe-options.interface.js";
import { createObserveModule } from "../observe.module.js";
import {
  FakeCollector,
  startFakeCollector,
} from "../testing/fake-collector.js";
import { waitFor } from "../testing/observe-harness.js";
import { Objective } from "./objective.decorator.js";

/** `RESTATE_EVERY_MS` in the registry. */
const RESTATE_EVERY_MS = 60 * 60 * 1000;

/** `MAX_OBJECTIVES_PER_BATCH` in the shared buffer - the collector's limit. */
const MAX_OBJECTIVES_PER_BATCH = 100;

// Taken before any test replaces it, so the moved clock can still read the
// real one.
const realNow = Date.now.bind(Date);

/**
 * Boots `controllers` under ObserveModule, reporting to `collector`, and sends
 * requests to it under trace ids of their own - so a test can wait for exactly
 * the batch that delivered a request, which is also the batch that delivered
 * any declaration the request brought due.
 */
async function bootApp(
  collector: FakeCollector,
  controllers: Type<unknown>[],
  overrides: Partial<ObserveOptions> = {},
) {
  const { ObserveModule, ObserveInstrument } = createObserveModule();

  @Module({
    imports: [
      ObserveModule.forRoot({
        appKey: "test-key",
        appSecret: "test-secret",
        serviceId: "delivery-app",
        serviceVersion: "objectives-int",
        endpoint: collector.url,
        flushInterval: 1000,
        runtimeMetrics: false,
        forwardLogs: false,
        ...overrides,
      }),
    ],
    controllers,
  })
  class DeliveryModule {}

  const app: INestApplication = await NestFactory.create(DeliveryModule, {
    instrument: ObserveInstrument,
    logger: false,
  });
  await app.listen(0);
  const baseUrl = await app.getUrl();
  let sent = 0;

  return {
    app,
    buffer: app.get(ObserveAgentSharedBuffer, { strict: false }),
    async get(path: string): Promise<string> {
      const traceId = `delivery-${++sent}`;
      const response = await fetch(`${baseUrl}${path}`, {
        headers: { "x-request-id": traceId },
      });
      await response.arrayBuffer();
      expect(response.status).toBe(200);
      return traceId;
    },
  };
}

type BootedApp = Awaited<ReturnType<typeof bootApp>>;

@Controller("heartbeat")
class HeartbeatController {
  @Get("quiet")
  @Objective({ availability: 99.9 })
  quiet() {
    return "ok";
  }

  @Get("restated")
  @Objective({ latency: { underMs: 200, target: 99 }, windowDays: 30 })
  restated() {
    return "ok";
  }

  @Get("lost")
  @Objective({ availability: 99.5, name: "Heartbeat" })
  lost() {
    return "ok";
  }
}

/**
 * A process states each route once and restates it hourly, so a batch that
 * never arrived costs an hour of the SLO rather than a release. Only a real
 * app shows the clock the registry reads is the one a request completes on,
 * and that the restatement rides out through the same batch as the request
 * that brought it due. The clock is moved by replacing `Date.now` alone: the
 * flush timer, the HTTP server and the worker thread all run on their own.
 */
describe("@Objective: restating a declaration over the life of a process", () => {
  let collector: FakeCollector;
  let booted: BootedApp;
  let clockAhead = 0;

  const declarationsOf = (handler: string) =>
    collector.declarations().filter((d) => d.handler === handler);

  beforeAll(async () => {
    collector = await startFakeCollector();
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clockAhead);
    booted = await bootApp(collector, [HeartbeatController]);
  });

  afterEach(() => {
    clockAhead = 0;
    collector.answerWith({});
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    await booted?.app.close();
    await collector?.close();
  });

  it("says nothing more about a route within the hour it was stated", async () => {
    const stated = await collector.waitForTrace(
      await booted.get("/heartbeat/quiet"),
    );
    expect(stated.objectives).toEqual([
      {
        handler: "HeartbeatController.quiet",
        operationId: "/heartbeat/quiet",
        method: "GET",
        objectives: [{ availability: 99.9 }],
      },
    ]);

    clockAhead = RESTATE_EVERY_MS - 60_000;
    const later = await collector.waitForTrace(
      await booted.get("/heartbeat/quiet"),
    );

    expect(later).not.toHaveProperty("objectives");
    expect(declarationsOf("HeartbeatController.quiet")).toHaveLength(1);
  });

  it("restates a route with its first request after the hour, then waits another hour", async () => {
    const first = await collector.waitForTrace(
      await booted.get("/heartbeat/restated"),
    );
    const [stated] = first.objectives!;

    clockAhead = RESTATE_EVERY_MS + 1_000;
    const hourLater = await collector.waitForTrace(
      await booted.get("/heartbeat/restated"),
    );
    const straightAfter = await collector.waitForTrace(
      await booted.get("/heartbeat/restated"),
    );

    expect(hourLater.objectives).toEqual([stated]);
    expect(straightAfter).not.toHaveProperty("objectives");
    expect(declarationsOf("HeartbeatController.restated")).toEqual([
      stated,
      stated,
    ]);
    expect(collector.violations).toEqual([]);
  });

  it("brings back a declaration lost with its batch, with the first request after the hour", async () => {
    // Sent, and refused: the worker drops a batch the collector will not
    // take, declarations and all.
    collector.answerWith({ message: "Service Unavailable" }, 503);
    const refused = await collector.waitForTrace(
      await booted.get("/heartbeat/lost"),
    );
    collector.answerWith({});
    const [lost] = refused.objectives!;
    expect(lost).toEqual({
      handler: "HeartbeatController.lost",
      operationId: "/heartbeat/lost",
      method: "GET",
      objectives: [{ availability: 99.5, name: "Heartbeat" }],
    });

    // The process cannot know it never arrived, so it waits out the hour...
    clockAhead = RESTATE_EVERY_MS - 60_000;
    const withinTheHour = await collector.waitForTrace(
      await booted.get("/heartbeat/lost"),
    );
    expect(withinTheHour).not.toHaveProperty("objectives");

    // ...and then says it again.
    clockAhead = RESTATE_EVERY_MS + 1_000;
    const afterTheHour = await collector.waitForTrace(
      await booted.get("/heartbeat/lost"),
    );
    expect(afterTheHour.objectives).toEqual([lost]);
  });
});

@Controller("trigger")
class TriggerController {
  @Get()
  fire() {
    return "ok";
  }
}

/** One declaring controller of many, each its own handler and route. */
function declaringController(index: number): Type<unknown> {
  class GeneratedController {
    handle() {
      return { index };
    }
  }
  // Its own name, which is what both the registry and the handler's span key
  // it by.
  Object.defineProperty(GeneratedController, "name", {
    value: `Route${index}Controller`,
  });
  const handle = Object.getOwnPropertyDescriptor(
    GeneratedController.prototype,
    "handle",
  )!;
  Get()(GeneratedController.prototype, "handle", handle);
  Objective({ availability: 99.9 })(
    GeneratedController.prototype,
    "handle",
    handle,
  );
  Controller(`routes/${index}`)(GeneratedController);
  return GeneratedController;
}

const DECLARING_ROUTES = 150;

/**
 * The collector refuses a batch carrying more than 100 declarations - whole.
 * An application with more declaring routes than that brings them all due
 * together when a deploy's traffic arrives, and a slow collector makes it
 * worse: while the worker waits on one answer, every flush is skipped and the
 * next batch collects everything. This drives exactly that - the collector
 * holds its answer while 150 declaring routes are hit - and shows the batch
 * that follows stays within the limit, and what did not fit is not lost.
 */
describe("@Objective: more declarations due than one batch may carry", () => {
  let collector: FakeCollector;
  let booted: BootedApp;

  /** One request to every declaring route, a few dozen at a time. */
  const requestEveryRoute = async (): Promise<string[]> => {
    const traceIds: string[] = [];
    for (let start = 0; start < DECLARING_ROUTES; start += 25) {
      const chunk = Array.from(
        { length: Math.min(25, DECLARING_ROUTES - start) },
        (_, offset) => booted.get(`/routes/${start + offset}`),
      );
      traceIds.push(...(await Promise.all(chunk)));
    }
    return traceIds;
  };

  beforeAll(async () => {
    collector = await startFakeCollector();
    booted = await bootApp(collector, [
      TriggerController,
      ...Array.from({ length: DECLARING_ROUTES }, (_, index) =>
        declaringController(index),
      ),
    ]);
  });

  afterAll(async () => {
    await booted?.app.close();
    await collector?.close();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("carries at most 100 per batch, and leaves the rest due for the next request to their routes", async () => {
    const answer = collector.holdAnswers();
    const trigger = await booted.get("/trigger");
    await collector.waitForTrace(trigger);

    // The worker is now waiting on the collector, holding the buffer.
    const inserted = vi.spyOn(booted.buffer, "insertRequestSnapshot");
    const firstRound = await requestEveryRoute();
    await waitFor(
      () => inserted.mock.calls.length >= DECLARING_ROUTES,
      5_000,
      "every request to be buffered",
    );
    answer.release();

    const together = await collector.waitForTrace(firstRound[0]);
    // Every first-round request rode in that one batch...
    expect(together.snapshots!.map((s) => s.ti).sort()).toEqual(
      [...firstRound].sort(),
    );
    // ...with as many of their declarations as the collector accepts.
    expect(together.objectives).toHaveLength(MAX_OBJECTIVES_PER_BATCH);
    expect(new Set(together.objectives!.map((d) => d.handler)).size).toBe(
      MAX_OBJECTIVES_PER_BATCH,
    );

    const secondRound = await requestEveryRoute();
    await Promise.all(
      secondRound.map((traceId) => collector.waitForTrace(traceId)),
    );

    // The other 50 arrived with the next request to their routes; none of
    // the 100 already stated came again.
    const handlers = collector.declarations().map((d) => d.handler);
    expect(handlers).toHaveLength(DECLARING_ROUTES);
    expect(new Set(handlers)).toEqual(
      new Set(
        Array.from(
          { length: DECLARING_ROUTES },
          (_, index) => `Route${index}Controller.handle`,
        ),
      ),
    );
    for (const batch of collector.batches) {
      expect(batch.objectives?.length ?? 0).toBeLessThanOrEqual(
        MAX_OBJECTIVES_PER_BATCH,
      );
    }
    expect(collector.violations).toEqual([]);
  });
});

@Controller("capped")
class CappedController {
  @Get("filler")
  filler() {
    return "ok";
  }

  @Get("declared")
  @Objective({ availability: 99.5 })
  declared() {
    return "ok";
  }
}

/**
 * `maxTracesPerBatch` drops a request outright once a batch is full. A
 * declaration is only marked stated when a batch actually takes it, so a
 * dropped request must not use up its route's declaration.
 */
describe("@Objective: a request the per-batch snapshot cap turns away", () => {
  let collector: FakeCollector;
  let booted: BootedApp;

  beforeAll(async () => {
    collector = await startFakeCollector();
    booted = await bootApp(collector, [TriggerController, CappedController], {
      maxTracesPerBatch: 2,
    });
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    await booted?.app.close();
    await collector?.close();
  });

  it("leaves its route's declaration due for the next request", async () => {
    const answer = collector.holdAnswers();
    await collector.waitForTrace(await booted.get("/trigger"));

    // Two requests fill the next batch, and the third - the first to a
    // declaring route - is turned away.
    const inserted = vi.spyOn(booted.buffer, "insertRequestSnapshot");
    const fillers = [
      await booted.get("/capped/filler"),
      await booted.get("/capped/filler"),
    ];
    await waitFor(() => inserted.mock.calls.length >= 2, 5_000, "the fillers");
    const turnedAway = await booted.get("/capped/declared");
    await waitFor(
      () => inserted.mock.calls.length >= 3,
      5_000,
      "the declaring request",
    );
    answer.release();

    const full = await collector.waitForTrace(fillers[0]);
    expect(full.snapshots!.map((s) => s.ti)).toEqual(fillers);
    expect(full).not.toHaveProperty("objectives");
    expect(collector.batchCarrying(turnedAway)).toBeUndefined();

    const admitted = await collector.waitForTrace(
      await booted.get("/capped/declared"),
    );
    expect(admitted.objectives).toEqual([
      {
        handler: "CappedController.declared",
        operationId: "/capped/declared",
        method: "GET",
        objectives: [{ availability: 99.5 }],
      },
    ]);
  });
});

@Controller("degraded")
class DegradedController {
  @Get("plain")
  plain() {
    return "ok";
  }

  @Get("orders")
  @Objective({ availability: 99.9 })
  orders() {
    return [];
  }
}

/**
 * An account past its event allowance has its span trees discarded, and the
 * agent stops sending them. Declarations are not spans: they have to keep
 * arriving, or the SLOs of exactly the accounts watching their usage go stale.
 */
describe("@Objective: while the collector is withholding span trees", () => {
  let collector: FakeCollector;
  let booted: BootedApp;

  beforeAll(async () => {
    collector = await startFakeCollector();
    collector.answerWith({ degraded: true });
    booted = await bootApp(collector, [DegradedController]);
  });

  afterAll(async () => {
    await booted?.app.close();
    await collector?.close();
  });

  it("still delivers a declaration, in a batch whose trees were withheld", async () => {
    await collector.waitForTrace(await booted.get("/degraded/plain"));
    await waitFor(
      () => booted.buffer.isDegraded(),
      5_000,
      "the agent to act on the degraded answer",
    );

    const traceId = await booted.get("/degraded/orders");
    const batch = await collector.waitForTrace(traceId);

    const snapshot = batch.snapshots!.find((s) => s.ti === traceId);
    expect(snapshot).toBeDefined();
    expect(snapshot).not.toHaveProperty("t");
    expect(batch.truncatedSpans).toBeGreaterThan(0);
    expect(batch.objectives).toEqual([
      {
        handler: "DegradedController.orders",
        operationId: "/degraded/orders",
        method: "GET",
        objectives: [{ availability: 99.9 }],
      },
    ]);
    // The declarations alone: the batch root also carries `truncatedSpans`,
    // which the SDK's copy of the contract does not declare.
    expect(
      validateTelemetryPayload(
        { serviceId: batch.serviceId, objectives: batch.objectives },
        { forbidUnknown: true },
      ),
    ).toEqual([]);
  });
});
