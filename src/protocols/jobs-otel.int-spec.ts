import { BullModule as BullClassicModule } from "@nestjs/bull";
import {
  InjectQueue as InjectBullQueue,
  Process,
  Processor as BullProcessor,
} from "@nestjs/bull";
import { BullModule, InjectQueue, Processor, WorkerHost } from "@nestjs/bullmq";
import {
  Controller,
  INestApplication,
  Injectable,
  Module,
  Post,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { Cron, Interval, ScheduleModule, Timeout } from "@nestjs/schedule";
import * as api from "@opentelemetry/api";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import type { Job as BullJob, Queue as BullQueue } from "bull";
import type { Job, Queue } from "bullmq";
import { randomBytes } from "crypto";
import { connect } from "net";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import {
  OtelTestSpans,
  installOtelGlobals,
  spanIdOf,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const REDIS_HOST = process.env.REDIS_HOST ?? "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);

/** Neither queue has an in-memory driver: those suites need Redis to answer. */
const redisReachable = await new Promise<boolean>((resolve) => {
  const socket = connect({ host: REDIS_HOST, port: REDIS_PORT });
  const finish = (reachable: boolean) => {
    socket.destroy();
    resolve(reachable);
  };
  socket.setTimeout(500, () => finish(false));
  socket.once("connect", () => finish(true));
  socket.once("error", () => finish(false));
});

/** Unique per run: other suites may share this Redis at the same time. */
const SUFFIX = `${process.pid}-${randomBytes(4).toString("hex")}`;
const MQ_QUEUE = `observe-otel-jobs-mq-${SUFFIX}`;
const BULL_QUEUE = `observe-otel-jobs-bull-${SUFFIX}`;

const spans = new OtelTestSpans();

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

/** The job names `jobs.ignore` matches, in every app below. */
const IGNORED = new Set(["ignored-mail", "ignored-report"]);
const observeOptions = {
  jobs: {
    tags: { environment: "test" },
    ignore: (job: { name: string }) => IGNORED.has(job.name),
  },
};

/** Every recorded run of the job named `name`, oldest first. */
function runsOf(name: string): ReadableSpan[] {
  return spans.finished
    .filter(
      (span) =>
        span.kind === api.SpanKind.CONSUMER &&
        span.attributes[ObserveAttributes.JOB_NAME] === name,
    )
    .sort((a, b) => toMillis(a.startTime) - toMillis(b.startTime));
}

/** Waits for `count` runs of `name` to have ended, and returns them. */
function waitForRuns(name: string, count = 1, timeoutMs = 5000) {
  return spans.waitFor(
    () => {
      const runs = runsOf(name);
      return runs.length >= count ? runs : undefined;
    },
    timeoutMs,
    `${count} run(s) of "${name}"`,
  );
}

/**
 * The finished spans of `span`'s trace. A run's root ends a tick after the
 * work inside it, so once the root is exported the whole trace is.
 */
function traceOfSpan(span: ReadableSpan): ReadableSpan[] {
  const { traceId } = span.spanContext();
  return spans.finished.filter(
    (candidate) => candidate.spanContext().traceId === traceId,
  );
}

function toMillis([seconds, nanos]: api.HrTime): number {
  return seconds * 1e3 + nanos / 1e6;
}

/** How long `span` took, in milliseconds. */
function durationOf(span: ReadableSpan): number {
  return toMillis(span.duration);
}

/** The one exception event on `span`. */
function exceptionOf(span: ReadableSpan) {
  const events = span.events.filter((event) => event.name === "exception");
  expect(events).toHaveLength(1);
  return events[0].attributes;
}

// ---------------------------------------------------------------- BullMQ ---

@Injectable()
class MailerService {
  deliver() {
    return "delivered";
  }
}

@Processor(MQ_QUEUE, { concurrency: 5 })
class MqMailProcessor extends WorkerHost {
  constructor(
    private readonly mailer: MailerService,
    @InjectQueue(MQ_QUEUE) private readonly queue: Queue,
  ) {
    super();
  }

  async process(job: Job) {
    if (job.name === "flaky-mail" && job.attemptsMade < 2) {
      throw new Error(`attempt ${job.attemptsMade + 1} failed`);
    }
    if (job.name === "first-mail") {
      await this.queue.add("follow-up-mail", {});
    }
    if (job.name === "slow-mail") {
      await new Promise((resolve) => setTimeout(resolve, 80));
    }
    return this.mailer.deliver();
  }
}

@Controller()
class MqController {
  constructor(@InjectQueue(MQ_QUEUE) private readonly queue: Queue) {}

  @Post("mq/signup")
  async signup() {
    await this.queue.add("welcome-mail", {});
    // Open while the worker - in this same process - starts the job, so a run
    // that wrongly continued this trace would land in it rather than beside.
    await new Promise((resolve) => setTimeout(resolve, 150));
    return { ok: true };
  }

  @Post("mq/delayed")
  async delayed() {
    await this.queue.add("delayed-mail", {}, { delay: 250 });
    return { ok: true };
  }

  @Post("mq/flaky")
  async flaky() {
    await this.queue.add(
      "flaky-mail",
      {},
      { attempts: 3, backoff: { type: "fixed", delay: 50 } },
    );
    return { ok: true };
  }

  @Post("mq/chain")
  async chain() {
    await this.queue.add("first-mail", {});
    return { ok: true };
  }

  @Post("mq/fan-out")
  async fanOut() {
    await this.queue.addBulk(
      Array.from({ length: 5 }, () => ({ name: "slow-mail", data: {} })),
    );
    // Open while all five run beside it.
    await new Promise((resolve) => setTimeout(resolve, 200));
    return { ok: true };
  }

  @Post("mq/schedule")
  async schedule() {
    await this.queue.upsertJobScheduler(
      `observe-otel-scheduler-${SUFFIX}`,
      { every: 150, limit: 2 },
      { name: "scheduled-mail", data: {} },
    );
    return { ok: true };
  }
}

@Module({
  imports: [
    BullModule.forRoot({
      connection: { host: REDIS_HOST, port: REDIS_PORT },
    }),
    BullModule.registerQueue({ name: MQ_QUEUE }),
    ObserveModule.forRoot(observeOptions),
  ],
  controllers: [MqController],
  providers: [MailerService, MqMailProcessor],
})
class MqTestModule {}

/**
 * BullMQ runs as OpenTelemetry traces: each run is a CONSUMER root of its own,
 * linked - not parented - to whatever span enqueued it.
 *
 * Ports the OTel-relevant scenarios of `queue-trace-inheritance` and
 * `bullmq-trace-scenarios`. Where the snapshot suites asserted a shared trace
 * id, these assert separate traces joined by links; the basic request-to-job
 * link itself is `otel-job-propagation.int-spec.ts`'s.
 */
describe.skipIf(!redisReachable)(
  "ObserveModule: BullMQ jobs as OpenTelemetry spans",
  () => {
    let app: NestExpressApplication;
    let queue: Queue;

    beforeAll(async () => {
      installOtelGlobals();
      app = await NestFactory.create<NestExpressApplication>(MqTestModule, {
        instrument: ObserveInstrument,
        logger: false,
      });
      await app.init();
      queue = app.get<Queue>(`BullQueue_${MQ_QUEUE}`);
    });

    afterAll(async () => {
      await queue
        ?.removeJobScheduler(`observe-otel-scheduler-${SUFFIX}`)
        .catch(() => false);
      await queue?.obliterate({ force: true }).catch(() => undefined);
      await app?.close();
      uninstallOtelGlobals();
    });

    // Each test waits for its own runs; earlier tests' are only in the way.
    beforeEach(() => spans.reset());

    const post = (path: string) =>
      request(app.getHttpServer()).post(path).expect(201);

    it("records a run as a CONSUMER root with the processor and its providers beneath it", async () => {
      await post("/mq/signup");

      const [run] = await waitForRuns("welcome-mail");
      // The request's root ends after its response: let it, before the next
      // test's reset, or it would be exported into that test's spans.
      await spans.traceOf("POST /mq/signup");
      expect(spanTree(traceOfSpan(run))).toBe(
        [
          `CONSUMER process ${MQ_QUEUE}`,
          "  INTERNAL MqMailProcessor.process",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
      expect(run.parentSpanContext).toBeUndefined();
      expect(run.status.code).toBe(api.SpanStatusCode.UNSET);
      expect(run.attributes).toMatchObject({
        "messaging.system": "bullmq",
        "messaging.operation.type": "process",
        "messaging.destination.name": MQ_QUEUE,
        "messaging.message.id": expect.any(String),
        [ObserveAttributes.JOB_NAME]: "welcome-mail",
        [ObserveAttributes.JOB_ATTEMPTS_MADE]: 0,
        [ObserveAttributes.JOB_WAIT_DURATION]: expect.any(Number),
        environment: "test",
      });
    });

    /**
     * The request stays open while the job runs, in the same process, so a run
     * that continued the request's trace instead of linking to it would land in
     * it - beneath whichever span was current.
     */
    it("keeps the enqueuer's trace to the enqueuer: none of the run's spans join it", async () => {
      await post("/mq/signup");

      const http = await spans.traceOf("POST /mq/signup");
      expect(spanTree(http)).toBe(
        [
          "SERVER POST /mq/signup",
          "  INTERNAL MqController.signup",
          "    INTERNAL Queue.add",
        ].join("\n"),
      );
      const [run] = await waitForRuns("welcome-mail");
      expect(run.spanContext().traceId).not.toBe(http[0].spanContext().traceId);
      expect(run.links.map((link) => link.context.spanId)).toEqual([
        spanIdOf(spanNamed(http, "Queue.add")),
      ]);
    });

    it("does not count a requested delay as time spent waiting", async () => {
      await post("/mq/delayed");

      const [run] = await waitForRuns("delayed-mail");
      expect(run.attributes[ObserveAttributes.JOB_WAIT_DURATION]).toBeLessThan(
        200,
      );
    });

    /**
     * A retry is a run of its own - its own trace, its own root - and each
     * attempt links back to the one request that enqueued the job. The failed
     * attempts carry the error on the processor span and ERROR on the root.
     */
    it("records each attempt of a retried job as its own trace, failures as ERROR, all linked to the enqueuer", async () => {
      await post("/mq/flaky");

      const attempts = await waitForRuns("flaky-mail", 3);
      const http = await spans.traceOf("POST /mq/flaky");
      const enqueuer = spanNamed(http, "Queue.add");

      expect(
        attempts.map((attempt) => [
          attempt.attributes[ObserveAttributes.JOB_ATTEMPTS_MADE],
          attempt.attributes[ObserveAttributes.JOB_MAX_ATTEMPTS],
          attempt.status.code,
        ]),
      ).toEqual([
        [0, 3, api.SpanStatusCode.ERROR],
        [1, 3, api.SpanStatusCode.ERROR],
        [2, 3, api.SpanStatusCode.UNSET],
      ]);
      expect(
        new Set(attempts.map((attempt) => attempt.spanContext().traceId)).size,
      ).toBe(3);
      for (const attempt of attempts) {
        expect(attempt.parentSpanContext).toBeUndefined();
        expect(attempt.links.map((link) => link.context)).toEqual([
          expect.objectContaining({
            traceId: enqueuer.spanContext().traceId,
            spanId: spanIdOf(enqueuer),
          }),
        ]);
      }

      // A failed attempt: the processor threw before reaching the mailer.
      const first = traceOfSpan(attempts[0]);
      expect(spanTree(first)).toBe(
        [
          `CONSUMER process ${MQ_QUEUE}`,
          "  INTERNAL MqMailProcessor.process",
        ].join("\n"),
      );
      expect(attempts[0].attributes["error.type"]).toBe("Error");
      const processor = spanNamed(first, "MqMailProcessor.process");
      expect(processor.status.code).toBe(api.SpanStatusCode.ERROR);
      expect(exceptionOf(processor)).toMatchObject({
        "exception.type": "Error",
        "exception.message": "attempt 1 failed",
      });

      // The attempt that succeeded reached the mailer, and is not an error.
      expect(spanTree(traceOfSpan(attempts[2]))).toBe(
        [
          `CONSUMER process ${MQ_QUEUE}`,
          "  INTERNAL MqMailProcessor.process",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
      expect(attempts[2].attributes["error.type"]).toBeUndefined();
    });

    /**
     * Request -> first-mail -> follow-up-mail: three traces, each run linked to
     * the span that was current where it was enqueued - the follow-up to the
     * processor span inside the first run, not to the request.
     */
    it("chains links through a job enqueued by a job", async () => {
      await post("/mq/chain");

      const [followUp] = await waitForRuns("follow-up-mail");
      const [first] = await waitForRuns("first-mail");
      const http = await spans.traceOf("POST /mq/chain");

      const firstTrace = traceOfSpan(first);
      expect(spanTree(firstTrace)).toBe(
        [
          `CONSUMER process ${MQ_QUEUE}`,
          "  INTERNAL MqMailProcessor.process",
          "    INTERNAL Queue.add",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
      expect(first.links.map((link) => link.context.spanId)).toEqual([
        spanIdOf(spanNamed(http, "Queue.add")),
      ]);
      expect(followUp.links.map((link) => link.context)).toEqual([
        expect.objectContaining({
          traceId: first.spanContext().traceId,
          spanId: spanIdOf(spanNamed(firstTrace, "Queue.add")),
        }),
      ]);

      const traceIds = new Set(
        [http[0], first, followUp].map((span) => span.spanContext().traceId),
      );
      expect(traceIds.size).toBe(3);
      // The first run's trace holds nothing of the follow-up's.
      expect(
        firstTrace.filter((span) => span.kind === api.SpanKind.CONSUMER),
      ).toEqual([first]);
    });

    it("starts an unlinked trace for a job enqueued outside any span", async () => {
      const added = await queue.add("orphan-mail", {});
      expect(added.opts).not.toHaveProperty("observeTraceContext");

      const [run] = await waitForRuns("orphan-mail");
      expect(run.parentSpanContext).toBeUndefined();
      expect(run.links).toEqual([]);
      expect(spanTree(traceOfSpan(run))).toBe(
        [
          `CONSUMER process ${MQ_QUEUE}`,
          "  INTERNAL MqMailProcessor.process",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
    });

    /**
     * Job options are writable by anything with Redis access: a forged id and
     * an all-zero - well-formed, but invalid - `traceparent` must not become
     * a link to a span that never existed.
     */
    it("ignores a stamped context that is not a valid span context", async () => {
      await queue.add("forged-mail", {}, {
        observeTraceId: "x".repeat(500),
        observeTraceContext: {
          traceparent: `00-${"0".repeat(32)}-${"0".repeat(16)}-01`,
        },
      } as never);

      const [run] = await waitForRuns("forged-mail");
      expect(run.links).toEqual([]);
      expect(run.parentSpanContext).toBeUndefined();
    });

    it("runs five jobs at once, each in a trace of its own with nothing leaked from a sibling", async () => {
      await post("/mq/fan-out");

      const runs = await waitForRuns("slow-mail", 5);
      const http = await spans.traceOf("POST /mq/fan-out");
      const enqueuer = spanIdOf(spanNamed(http, "Queue.addBulk"));

      expect(new Set(runs.map((run) => run.spanContext().traceId)).size).toBe(
        5,
      );
      for (const run of runs) {
        expect(spanTree(traceOfSpan(run))).toBe(
          [
            `CONSUMER process ${MQ_QUEUE}`,
            "  INTERNAL MqMailProcessor.process",
            "    INTERNAL MailerService.deliver",
          ].join("\n"),
        );
        expect(run.links.map((link) => link.context.spanId)).toEqual([
          enqueuer,
        ]);
      }
      expect(spanTree(http)).toBe(
        [
          "SERVER POST /mq/fan-out",
          "  INTERNAL MqController.fanOut",
          "    INTERNAL Queue.addBulk",
        ].join("\n"),
      );
    });

    /**
     * A scheduler's firings are not caused by the request that registered the
     * schedule - linking every repetition to it would tie a schedule's whole
     * life to one long-gone request.
     */
    it("gives every firing of a job scheduler a trace of its own, unlinked", async () => {
      await post("/mq/schedule");

      const firings = await waitForRuns("scheduled-mail", 2);
      expect(
        new Set(firings.map((firing) => firing.spanContext().traceId)).size,
      ).toBe(firings.length);
      for (const firing of firings) {
        expect(firing.links).toEqual([]);
        expect(firing.parentSpanContext).toBeUndefined();
      }
    });
  },
);

// ------------------------------------------------------------ classic Bull ---

/** How many times the processor `jobs.ignore` matches has run. */
let ignoredBullRuns = 0;

@BullProcessor(BULL_QUEUE)
class BullMailProcessor {
  constructor(private readonly mailer: MailerService) {}

  @Process("welcome-mail")
  async welcome(_job: BullJob) {
    return this.mailer.deliver();
  }

  @Process("bulk-mail")
  async bulk(_job: BullJob) {
    return this.mailer.deliver();
  }

  @Process("orphan-mail")
  async orphan(_job: BullJob) {
    return this.mailer.deliver();
  }

  @Process("ignored-mail")
  async ignored(_job: BullJob) {
    ignoredBullRuns++;
    return this.mailer.deliver();
  }

  @Process("after-ignored-mail")
  async afterIgnored(_job: BullJob) {
    return this.mailer.deliver();
  }

  @Process("repeat-mail")
  async repeat(_job: BullJob) {
    return this.mailer.deliver();
  }

  @Process("failing-mail")
  async failing(_job: BullJob) {
    throw new Error("deliberate");
  }

  // Two parameters: Bull reads the arity and waits for `done`, not a promise.
  @Process("callback-mail")
  callback(_job: BullJob, done: (error?: Error | null) => void) {
    setTimeout(() => done(), 20);
  }

  @Process("callback-failing-mail")
  callbackFailing(_job: BullJob, done: (error?: Error | null) => void) {
    setTimeout(() => done(new RangeError("mailbox full")), 5);
  }
}

@Controller()
class BullController {
  constructor(@InjectBullQueue(BULL_QUEUE) private readonly queue: BullQueue) {}

  @Post("bull/signup")
  async signup() {
    await this.queue.add("welcome-mail", {});
    await new Promise((resolve) => setTimeout(resolve, 150));
    return { ok: true };
  }

  @Post("bull/repeat")
  async repeat() {
    await this.queue.add(
      "repeat-mail",
      {},
      { repeat: { every: 150, limit: 2 } },
    );
    return { ok: true };
  }

  @Post("bull/bulk")
  async bulk() {
    await this.queue.addBulk([{ name: "bulk-mail", data: {} }]);
    return { ok: true };
  }
}

@Module({
  imports: [
    BullClassicModule.forRoot({
      redis: { host: REDIS_HOST, port: REDIS_PORT },
    }),
    BullClassicModule.registerQueue({ name: BULL_QUEUE }),
    ObserveModule.forRoot(observeOptions),
  ],
  controllers: [BullController],
  providers: [MailerService, BullMailProcessor],
})
class BullTestModule {}

/**
 * The original Bull behind `@nestjs/bull`, recorded through OpenTelemetry:
 * the same CONSUMER root and link as BullMQ, by way of a different patch
 * (`Queue#setHandler`, and `add(name?, data, opts)` for the stamp).
 *
 * Ports `bull-collection.int-spec.ts`.
 */
describe.skipIf(!redisReachable)(
  "ObserveModule: Bull jobs as OpenTelemetry spans",
  () => {
    let app: NestExpressApplication;
    let queue: BullQueue;

    beforeAll(async () => {
      installOtelGlobals();
      app = await NestFactory.create<NestExpressApplication>(BullTestModule, {
        instrument: ObserveInstrument,
        logger: false,
      });
      await app.init();
      queue = app.get<BullQueue>(`BullQueue_${BULL_QUEUE}`);
    });

    afterAll(async () => {
      await queue?.obliterate({ force: true }).catch(() => undefined);
      await queue?.close().catch(() => undefined);
      await app?.close();
      uninstallOtelGlobals();
    });

    beforeEach(() => spans.reset());

    /** Bull runs a named job through the handler registered for that name. */
    const bullRunsOf = (name: string, count = 1) =>
      spans.waitFor(
        () => {
          const runs = runsOf(name).filter(
            (run) =>
              run.attributes["messaging.destination.name"] === BULL_QUEUE,
          );
          return runs.length >= count ? runs : undefined;
        },
        5000,
        `${count} Bull run(s) of "${name}"`,
      );

    it("records a run as a CONSUMER root linked to the request that enqueued it", async () => {
      await request(app.getHttpServer()).post("/bull/signup").expect(201);

      const [run] = await bullRunsOf("welcome-mail");
      const http = await spans.traceOf("POST /bull/signup");

      expect(spanTree(traceOfSpan(run))).toBe(
        [
          `CONSUMER process ${BULL_QUEUE}`,
          "  INTERNAL BullMailProcessor.welcome",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
      expect(spanTree(http)).toBe(
        [
          "SERVER POST /bull/signup",
          "  INTERNAL BullController.signup",
          "    INTERNAL Queue.add",
          "      INTERNAL Queue.isReady",
        ].join("\n"),
      );
      expect(run.parentSpanContext).toBeUndefined();
      expect(run.links.map((link) => link.context.spanId)).toEqual([
        spanIdOf(spanNamed(http, "Queue.add")),
      ]);
      expect(run.attributes).toMatchObject({
        "messaging.system": "bull",
        "messaging.operation.type": "process",
        "messaging.destination.name": BULL_QUEUE,
        "messaging.message.id": expect.any(String),
        [ObserveAttributes.JOB_NAME]: "welcome-mail",
        [ObserveAttributes.JOB_ATTEMPTS_MADE]: 0,
        [ObserveAttributes.JOB_WAIT_DURATION]: expect.any(Number),
      });
    });

    it("links through addBulk as well", async () => {
      await request(app.getHttpServer()).post("/bull/bulk").expect(201);

      const [run] = await bullRunsOf("bulk-mail");
      const http = await spans.traceOf("POST /bull/bulk");
      expect(run.links.map((link) => link.context.spanId)).toEqual([
        spanIdOf(spanNamed(http, "Queue.addBulk")),
      ]);
    });

    /**
     * Bull copies a repeatable job's options to every repetition, so a stamp
     * on the registering `add` would link the schedule's whole life to one
     * request. Each repetition is a firing of its own.
     */
    it("gives every repetition of a repeatable job a trace of its own, unlinked", async () => {
      await request(app.getHttpServer()).post("/bull/repeat").expect(201);

      const firings = await bullRunsOf("repeat-mail", 2);
      expect(
        new Set(firings.map((firing) => firing.spanContext().traceId)).size,
      ).toBe(firings.length);
      for (const firing of firings) {
        expect(firing.links).toEqual([]);
        expect(firing.parentSpanContext).toBeUndefined();
      }
    });

    it("starts an unlinked trace for a job enqueued outside any span", async () => {
      await queue.add("orphan-mail", {});

      const [run] = await bullRunsOf("orphan-mail");
      expect(run.links).toEqual([]);
      expect(run.parentSpanContext).toBeUndefined();
    });

    it("records nothing for a run matched by jobs.ignore", async () => {
      await queue.add("ignored-mail", {});
      await spans.waitFor(
        () => (ignoredBullRuns === 1 ? true : undefined),
        5000,
        "the ignored-mail run",
      );

      // Enqueued once the ignored run is under way: had it a span, it would
      // be exported well before this job has made the round trip through Redis.
      await queue.add("after-ignored-mail", {});
      const [after] = await bullRunsOf("after-ignored-mail");
      expect(runsOf("ignored-mail")).toEqual([]);
      expect(
        spans.finished.filter(
          (span) => span.name === "BullMailProcessor.ignored",
        ),
      ).toEqual([]);
      expect(spanTree(traceOfSpan(after))).toBe(
        [
          `CONSUMER process ${BULL_QUEUE}`,
          "  INTERNAL BullMailProcessor.afterIgnored",
          "    INTERNAL MailerService.deliver",
        ].join("\n"),
      );
    });

    it("records every attempt of a failing processor as ERROR, with its attempt number", async () => {
      await queue.add("failing-mail", {}, { attempts: 2 });

      const attempts = await bullRunsOf("failing-mail", 2);
      expect(
        attempts.map((attempt) => [
          attempt.attributes[ObserveAttributes.JOB_ATTEMPTS_MADE],
          attempt.attributes[ObserveAttributes.JOB_MAX_ATTEMPTS],
          attempt.status.code,
          attempt.attributes["error.type"],
        ]),
      ).toEqual([
        [0, 2, api.SpanStatusCode.ERROR, "Error"],
        [1, 2, api.SpanStatusCode.ERROR, "Error"],
      ]);
      const trace = traceOfSpan(attempts[1]);
      expect(spanTree(trace)).toBe(
        [
          `CONSUMER process ${BULL_QUEUE}`,
          "  INTERNAL BullMailProcessor.failing",
        ].join("\n"),
      );
      expect(
        exceptionOf(spanNamed(trace, "BullMailProcessor.failing")),
      ).toMatchObject({ "exception.message": "deliberate" });
      // On the processor's span, where it was thrown - not again on the run.
      expect(attempts[1].events).toEqual([]);
    });

    /**
     * The handler returned before failing, so no span saw the error: it
     * reached the driver through `done`, and belongs on the run.
     */
    it("records the error a callback-style processor passed to `done` on the run", async () => {
      await queue.add("callback-failing-mail", {});

      const [run] = await bullRunsOf("callback-failing-mail");
      expect(run.status).toEqual({
        code: api.SpanStatusCode.ERROR,
        message: "mailbox full",
      });
      expect(run.attributes["error.type"]).toBe("RangeError");
      expect(exceptionOf(run)).toMatchObject({
        "exception.type": "RangeError",
        "exception.message": "mailbox full",
      });
      const handler = spanNamed(
        traceOfSpan(run),
        "BullMailProcessor.callbackFailing",
      );
      expect(handler.status.code).toBe(api.SpanStatusCode.UNSET);
    });

    /**
     * The handler returns at once and calls `done` later; the run is the time
     * to `done`, so the root has to stay open past the handler's own span.
     */
    it("keeps a callback-style run open until `done`", async () => {
      await queue.add("callback-mail", {});

      const [run] = await bullRunsOf("callback-mail");
      expect(run.status.code).toBe(api.SpanStatusCode.UNSET);
      expect(durationOf(run)).toBeGreaterThanOrEqual(15);
      expect(spanTree(traceOfSpan(run))).toBe(
        [
          `CONSUMER process ${BULL_QUEUE}`,
          "  INTERNAL BullMailProcessor.callback",
        ].join("\n"),
      );
    });
  },
);

// ------------------------------------------------------- @nestjs/schedule ---

@Injectable()
class LedgerService {
  reconcile() {
    return "reconciled";
  }
}

@Injectable()
class TasksService {
  constructor(private readonly ledger: LedgerService) {}

  ignoredRuns = 0;

  @Timeout(20)
  async nightlyReport() {
    await new Promise((resolve) => setTimeout(resolve, 5));
    return this.ledger.reconcile();
  }

  @Timeout("explode", 20)
  async explode() {
    await Promise.resolve();
    throw new Error("deliberate");
  }

  @Interval("heartbeat", 50)
  heartbeat() {
    return "ok";
  }

  @Cron("* * * * * *", { name: "every-second" })
  everySecond() {
    return this.ledger.reconcile();
  }

  @Timeout("ignored-report", 20)
  ignoredReport() {
    this.ignoredRuns++;
    return this.ledger.reconcile();
  }
}

@Module({
  imports: [ScheduleModule.forRoot(), ObserveModule.forRoot(observeOptions)],
  providers: [LedgerService, TasksService],
})
class ScheduleTestModule {}

/**
 * `@nestjs/schedule` firings as OpenTelemetry traces. A firing is a job run
 * like any queue's: a CONSUMER root named `process <scheduler type>`, the
 * handler's name - explicit, or `Class.method` - as the job name, and the
 * handler itself as an INTERNAL span beneath it. A timer enqueued it, so there
 * is never a link.
 *
 * Ports `schedule-collection.int-spec.ts`; in the snapshot model the handler
 * was the root span, here it sits under the run's root.
 */
describe("ObserveModule: @nestjs/schedule handlers as OpenTelemetry spans", () => {
  let app: INestApplication;

  beforeAll(async () => {
    installOtelGlobals();
    app = await NestFactory.create(ScheduleTestModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    uninstallOtelGlobals();
  });

  it("records a @Timeout firing as a CONSUMER root with the handler and its providers beneath it", async () => {
    const [run] = await waitForRuns("TasksService.nightlyReport");

    expect(spanTree(traceOfSpan(run))).toBe(
      [
        "CONSUMER process timeout",
        "  INTERNAL TasksService.nightlyReport",
        "    INTERNAL LedgerService.reconcile",
      ].join("\n"),
    );
    expect(run.parentSpanContext).toBeUndefined();
    expect(run.links).toEqual([]);
    expect(run.status.code).toBe(api.SpanStatusCode.UNSET);
    expect(durationOf(run)).toBeGreaterThanOrEqual(4);
    expect(run.attributes).toMatchObject({
      "messaging.operation.type": "process",
      "messaging.destination.name": "timeout",
      "messaging.message.id": expect.stringMatching(/^[0-9a-f-]{36}$/),
      [ObserveAttributes.JOB_NAME]: "TasksService.nightlyReport",
      environment: "test",
    });
    // A timer has no queue: no broker, no attempts, no wait.
    expect(run.attributes).not.toHaveProperty("messaging.system");
    expect(run.attributes).not.toHaveProperty(
      ObserveAttributes.JOB_ATTEMPTS_MADE,
    );
    expect(run.attributes).not.toHaveProperty(
      ObserveAttributes.JOB_WAIT_DURATION,
    );
  });

  it("records a throwing handler as ERROR, with the exception on the handler's span", async () => {
    const [run] = await waitForRuns("explode");
    const trace = traceOfSpan(run);

    expect(spanTree(trace)).toBe(
      ["CONSUMER process timeout", "  INTERNAL TasksService.explode"].join(
        "\n",
      ),
    );
    expect(run.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(run.attributes["error.type"]).toBe("Error");
    const handler = spanNamed(trace, "TasksService.explode");
    expect(handler.status).toEqual({
      code: api.SpanStatusCode.ERROR,
      message: "deliberate",
    });
    expect(exceptionOf(handler)).toMatchObject({
      "exception.type": "Error",
      "exception.message": "deliberate",
    });
  });

  it("records every @Interval firing as a trace of its own", async () => {
    const firings = await waitForRuns("heartbeat", 2);

    for (const firing of firings) {
      expect(spanTree(traceOfSpan(firing))).toBe(
        ["CONSUMER process interval", "  INTERNAL TasksService.heartbeat"].join(
          "\n",
        ),
      );
      expect(firing.links).toEqual([]);
    }
    expect(
      new Set(firings.map((firing) => firing.spanContext().traceId)).size,
    ).toBe(firings.length);
    expect(
      new Set(
        firings.map((firing) => firing.attributes["messaging.message.id"]),
      ).size,
    ).toBe(firings.length);
  });

  it("records a @Cron firing under the cron job's name", async () => {
    const [run] = await waitForRuns("every-second", 1, 5000);

    expect(spanTree(traceOfSpan(run))).toBe(
      [
        "CONSUMER process cron",
        "  INTERNAL TasksService.everySecond",
        "    INTERNAL LedgerService.reconcile",
      ].join("\n"),
    );
    expect(run.attributes["messaging.destination.name"]).toBe("cron");
  });

  it("records nothing for a handler matched by jobs.ignore", async () => {
    // Fires alongside nightlyReport, so once that one has been exported the
    // ignored one has had every chance to be.
    await waitForRuns("TasksService.nightlyReport");
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(app.get(TasksService).ignoredRuns).toBe(1);
    expect(runsOf("ignored-report")).toEqual([]);
    expect(
      spans.finished.filter(
        (span) => span.name === "TasksService.ignoredReport",
      ),
    ).toEqual([]);
  });
});

// ------------------------------------------- a second app, same process ---

/** A module of its own, with a tracer provider of its own. */
const secondSpans = new OtelTestSpans();
const second = createObserveModule({
  opentelemetry: { tracerProvider: secondSpans.provider },
});

@Injectable()
class SecondTasksService {
  constructor(private readonly ledger: LedgerService) {}

  @Timeout(20)
  async tick() {
    return this.ledger.reconcile();
  }
}

@Module({
  imports: [ScheduleModule.forRoot(), second.ObserveModule.forRoot({})],
  providers: [LedgerService, SecondTasksService],
})
class SecondScheduleTestModule {}

/**
 * A second application booted in the same process once the first has closed
 * - an e2e suite with one app per file section, say - records its scheduled
 * handlers through its own module, as the queue agents already do: they park
 * the original method and replace their wrapper on every boot.
 *
 * Once marked patched the explorer used to stay the first app's for the
 * life of the process, its firings recorded into the first provider.
 */
describe("ObserveModule: @nestjs/schedule in a second app in the same process", () => {
  let app: INestApplication;

  beforeAll(async () => {
    installOtelGlobals();
    app = await NestFactory.create(SecondScheduleTestModule, {
      instrument: second.ObserveInstrument,
      logger: false,
    });
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    uninstallOtelGlobals();
  });

  it("records the second app's firings through the second app's module", async () => {
    const root = await secondSpans.waitFor(
      (finished) =>
        finished.find(
          (span) =>
            span.attributes[ObserveAttributes.JOB_NAME] ===
            "SecondTasksService.tick",
        ),
      2000,
      "the second app's tick run",
    );
    expect(
      spanTree(
        secondSpans.finished.filter(
          (span) => span.spanContext().traceId === root.spanContext().traceId,
        ),
      ),
    ).toBe(
      [
        "CONSUMER process timeout",
        "  INTERNAL SecondTasksService.tick",
        "    INTERNAL LedgerService.reconcile",
      ].join("\n"),
    );
  });
});
