import {
  BullModule,
  InjectQueue,
  Processor,
  WorkerHost,
  getQueueToken,
} from "@nestjs/bullmq";
import {
  Controller,
  Inject,
  Injectable,
  Module,
  OnApplicationShutdown,
  OnModuleInit,
  Post,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { Job, JobsOptions, Queue, Worker } from "bullmq";
import { connect } from "net";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import {
  CollectedJobSnapshots,
  collectJobSnapshots,
  testObserveOptions,
  waitForJobSnapshot,
} from "../testing/observe-harness.js";

const REDIS_HOST = process.env.REDIS_HOST ?? "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);
const connection = { host: REDIS_HOST, port: REDIS_PORT };

/** BullMQ has no in-memory driver, so this suite runs only where Redis answers. */
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

/**
 * `@taskforcesh/bullmq-pro` is published only to Taskforce's private registry,
 * so the agent is handed the stand-ins below in its place. Everything else it
 * loads goes through the real loader.
 */
const bullmqPro = vi.hoisted(() => ({
  module: undefined as Record<string, unknown> | undefined,
}));

vi.mock("../utils/optional-peer.util.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../utils/optional-peer.util.js")>();
  return {
    ...actual,
    loadOptionalPeer: (packageName: string, specifier?: string) =>
      packageName === "@taskforcesh/bullmq-pro"
        ? { installed: true, module: bullmqPro.module }
        : actual.loadOptionalPeer(packageName, specifier),
  };
});

/**
 * Shaped after the BullMQ Pro API reference: both classes extend BullMQ's own,
 * `WorkerPro` runs jobs through the `processJob` it inherits, and `QueuePro`
 * overrides `add` and `addBulk`. These overrides go through `addJob` - the
 * seam BullMQ keeps for subclasses that wrap `add` - and never reach
 * `Queue.prototype.add`, the case in which a patch on BullMQ's queue alone
 * would leave every job without the trace that enqueued it.
 */
class WorkerPro extends Worker {}

/** Job names that went through `QueuePro`'s own overrides, not around them. */
const addedThroughQueuePro: string[] = [];

class QueuePro extends Queue {
  override add(name: string, data: unknown, opts?: JobsOptions) {
    addedThroughQueuePro.push(name);
    return this.addJob(name, data, opts);
  }

  override addBulk(
    jobs: Array<{ name: string; data: unknown; opts?: JobsOptions }>,
  ) {
    addedThroughQueuePro.push(...jobs.map((job) => job.name));
    return Promise.all(
      jobs.map((job) => this.addJob(job.name, job.data, job.opts)),
    );
  }
}

bullmqPro.module = { WorkerPro, QueuePro };

const UUID = /^[0-9a-f-]{36}$/;

/**
 * The lock token each processor was handed, by job name. A processor that
 * moves its own job - `moveToDelayed`, `extendLock` - cannot do it without.
 */
const tokensSeen = new Map<string, unknown>();

@Injectable()
class TranscoderService {
  transcode() {
    return "transcoded";
  }
}

const FORK_QUEUE = `observe-pro-fork-${process.pid}`;
const FORK_QUEUE_TOKEN = "FORK_QUEUE";

@Injectable()
class AudioProcessor {
  constructor(private readonly transcoder: TranscoderService) {}

  async process(job: Job, token?: string) {
    tokensSeen.set(job.name, token);
    if (job.name === "broken-audio") {
      throw new Error("cannot transcode");
    }
    return this.transcoder.transcode();
  }
}

/**
 * What `@taskforcesh/nestjs-bullmq-pro`'s explorer does with a processor: it
 * binds `process` on the provider instance and hands it straight to a
 * `WorkerPro`. The fork predates `@nestjs/bullmq`'s processor decorator, so
 * nothing sits in between - the reason its jobs went unobserved.
 */
@Injectable()
class ForkExplorer implements OnModuleInit, OnApplicationShutdown {
  private worker?: WorkerPro;

  constructor(private readonly processor: AudioProcessor) {}

  onModuleInit() {
    this.worker = new WorkerPro(
      FORK_QUEUE,
      this.processor.process.bind(this.processor),
      { connection },
    );
  }

  async onApplicationShutdown() {
    await this.worker?.close();
  }
}

@Controller()
class AudioController {
  constructor(@Inject(FORK_QUEUE_TOKEN) private readonly queue: QueuePro) {}

  @Post("transcode")
  async transcode() {
    await this.queue.add("transcode-audio", {});
    return { ok: true };
  }

  @Post("transcode-bulk")
  async transcodeBulk() {
    await this.queue.addBulk([{ name: "bulk-audio", data: {} }]);
    return { ok: true };
  }

  @Post("broken")
  async broken() {
    await this.queue.add("broken-audio", {});
    return { ok: true };
  }
}

const forkObserve = createObserveModule();

@Module({
  imports: [forkObserve.ObserveModule.forRoot(testObserveOptions())],
  controllers: [AudioController],
  providers: [
    TranscoderService,
    AudioProcessor,
    ForkExplorer,
    {
      provide: FORK_QUEUE_TOKEN,
      useFactory: () => new QueuePro(FORK_QUEUE, { connection }),
    },
  ],
})
class ForkWiredModule {}

/**
 * The other way to run BullMQ Pro in Nest: `@nestjs/bullmq` itself, told to
 * build Pro queues and workers. Set before `registerQueue()` below is
 * evaluated, which is when the queue class is read.
 */
BullModule.queueClass = QueuePro;
BullModule.workerClass = WorkerPro;

const NEST_QUEUE = `observe-pro-nest-${process.pid}`;

@Processor(NEST_QUEUE)
class VideoProcessor extends WorkerHost {
  constructor(private readonly transcoder: TranscoderService) {
    super();
  }

  async process(job: Job, token?: string) {
    tokensSeen.set(job.name, token);
    return this.transcoder.transcode();
  }
}

@Controller()
class VideoController {
  constructor(@InjectQueue(NEST_QUEUE) private readonly queue: QueuePro) {}

  @Post("video")
  async video() {
    await this.queue.add("transcode-video", {});
    return { ok: true };
  }
}

const nestObserve = createObserveModule();

@Module({
  imports: [
    BullModule.forRoot({ connection }),
    BullModule.registerQueue({ name: NEST_QUEUE }),
    nestObserve.ObserveModule.forRoot(testObserveOptions()),
  ],
  controllers: [VideoController],
  providers: [TranscoderService, VideoProcessor],
})
class NestBullMQWiredModule {}

/**
 * Job tracing for BullMQ Pro, which `@taskforcesh/nestjs-bullmq-pro` runs on.
 * Both wirings run the job on a `WorkerPro` and enqueue it through a
 * `QueuePro`, and only a real queue shows the trace id crossing Redis.
 */
describe.skipIf(!redisReachable)("ObserveModule: BullMQ Pro", () => {
  afterAll(() => {
    BullModule.queueClass = Queue;
    BullModule.workerClass = Worker;
  });

  describe("wired the way @taskforcesh/nestjs-bullmq-pro wires it", () => {
    let app: NestExpressApplication;
    let jobs: CollectedJobSnapshots;

    beforeAll(async () => {
      app = await NestFactory.create<NestExpressApplication>(ForkWiredModule, {
        instrument: forkObserve.ObserveInstrument,
        logger: false,
      });
      jobs = collectJobSnapshots(app);
      await app.init();
    });

    afterAll(async () => {
      const queue = app?.get<QueuePro>(FORK_QUEUE_TOKEN);
      await queue?.obliterate({ force: true }).catch(() => undefined);
      await app?.close();
      await queue?.close();
    });

    it("traces a job a WorkerPro runs, with the processor's calls as its spans", async () => {
      await request(app.getHttpServer())
        .post("/transcode")
        .set("x-request-id", "pro-trace-1")
        .expect(201);

      const job = await waitForJobSnapshot(
        jobs,
        (item) => item.name === "transcode-audio",
      );
      expect(job.queueName).toBe(FORK_QUEUE);
      expect(job.status).toBe("completed");
      expect(job.traces[0]).toMatchObject({
        className: "AudioProcessor",
        methodKey: "process",
      });
      expect(job.traces[0].children).toEqual([
        expect.objectContaining({
          className: "TranscoderService",
          methodKey: "transcode",
        }),
      ]);
      expect(tokensSeen.get("transcode-audio")).toEqual(expect.any(String));
    });

    it("runs the job under the id of the request that enqueued it through QueuePro#add", async () => {
      await request(app.getHttpServer())
        .post("/transcode")
        .set("x-request-id", "pro-trace-2")
        .expect(201);

      const job = await waitForJobSnapshot(
        jobs,
        (item) =>
          item.name === "transcode-audio" && item.traceId === "pro-trace-2",
      );
      expect(job.status).toBe("completed");
      // Stamped on the way into QueuePro's own `add`, not by routing around
      // it: the overrides are where Pro's group options are handled.
      expect(addedThroughQueuePro).toContain("transcode-audio");
    });

    it("inherits through QueuePro#addBulk as well", async () => {
      await request(app.getHttpServer())
        .post("/transcode-bulk")
        .set("x-request-id", "pro-bulk-1")
        .expect(201);

      const job = await waitForJobSnapshot(
        jobs,
        (item) => item.name === "bulk-audio",
      );
      expect(job.traceId).toBe("pro-bulk-1");
      expect(addedThroughQueuePro).toContain("bulk-audio");
    });

    it("reports a failed run as failed, with the error on its root span", async () => {
      await request(app.getHttpServer())
        .post("/broken")
        .set("x-request-id", "pro-broken-1")
        .expect(201);

      const job = await waitForJobSnapshot(
        jobs,
        (item) => item.name === "broken-audio",
      );
      expect(job.traceId).toBe("pro-broken-1");
      expect(job.status).toBe("failed");
      expect(job.traces[0].error).toBeTruthy();
    });

    it("mints a fresh id for a job enqueued outside any trace", async () => {
      const queue = app.get<QueuePro>(FORK_QUEUE_TOKEN);
      await queue.add("orphan-audio", {});

      const job = await waitForJobSnapshot(
        jobs,
        (item) => item.name === "orphan-audio",
      );
      expect(job.traceId).toMatch(UUID);
    });
  });

  describe("wired through @nestjs/bullmq with the Pro classes", () => {
    let app: NestExpressApplication;
    let jobs: CollectedJobSnapshots;

    beforeAll(async () => {
      app = await NestFactory.create<NestExpressApplication>(
        NestBullMQWiredModule,
        { instrument: nestObserve.ObserveInstrument, logger: false },
      );
      jobs = collectJobSnapshots(app);
      await app.init();
    });

    afterAll(async () => {
      const queue = app?.get<QueuePro>(getQueueToken(NEST_QUEUE));
      await queue?.obliterate({ force: true }).catch(() => undefined);
      await app?.close();
    });

    it("builds Pro queues and workers, so the run below is a WorkerPro's", () => {
      expect(app.get(getQueueToken(NEST_QUEUE))).toBeInstanceOf(QueuePro);
      expect(app.get(VideoProcessor).worker).toBeInstanceOf(WorkerPro);
    });

    it("reports each run once, with its spans, though both the processor decorator and the worker are instrumented", async () => {
      await request(app.getHttpServer())
        .post("/video")
        .set("x-request-id", "video-trace-1")
        .expect(201);

      const job = await waitForJobSnapshot(
        jobs,
        (item) => item.name === "transcode-video",
      );
      expect(job.traceId).toBe("video-trace-1");
      expect(job.status).toBe("completed");
      expect(job.traces[0]).toMatchObject({
        className: "VideoProcessor",
        methodKey: "process",
      });
      expect(job.traces[0].children).toEqual([
        expect.objectContaining({
          className: "TranscoderService",
          methodKey: "transcode",
        }),
      ]);

      expect(tokensSeen.get("transcode-video")).toEqual(expect.any(String));

      // A second layer that opened a trace of its own would report the same
      // run again; give it the time to.
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(
        jobs.items.filter((item) => item.name === "transcode-video"),
      ).toHaveLength(1);
    });
  });
});
