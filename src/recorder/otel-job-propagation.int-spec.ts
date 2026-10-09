import { InjectQueue, BullModule, Processor, WorkerHost } from "@nestjs/bullmq";
import { Controller, Injectable, Module, Post } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import * as api from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import type { Job, Queue } from "bullmq";
import { connect } from "net";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { testObserveOptions } from "../testing/observe-harness.js";

const REDIS_HOST = process.env.REDIS_HOST ?? "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);

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

const QUEUE_NAME = `observe-otel-int-${process.pid}`;

const exporter = new InMemorySpanExporter();
const provider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
});

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: {
    tracerProvider: provider,
    propagator: new W3CTraceContextPropagator(),
  },
});

@Injectable()
class MailerService {
  deliver() {
    return "delivered";
  }
}

@Processor(QUEUE_NAME)
class MailProcessor extends WorkerHost {
  constructor(private readonly mailer: MailerService) {
    super();
  }

  async process(_job: Job) {
    return this.mailer.deliver();
  }
}

@Controller()
class SignupController {
  constructor(@InjectQueue(QUEUE_NAME) private readonly queue: Queue) {}

  @Post("signup")
  async signup() {
    await this.queue.add("welcome-mail", {});
    return { ok: true };
  }
}

@Module({
  imports: [
    BullModule.forRoot({
      connection: { host: REDIS_HOST, port: REDIS_PORT },
    }),
    BullModule.registerQueue({ name: QUEUE_NAME }),
    ObserveModule.forRoot(testObserveOptions()),
  ],
  controllers: [SignupController],
  providers: [MailerService, MailProcessor],
})
class QueueTestModule {}

/**
 * The enqueuing span's context crosses Redis in the job's options, and the
 * run links back to it - only a real queue proves the options survive
 * BullMQ's encoding.
 */
describe.skipIf(!redisReachable)(
  "ObserveModule: OpenTelemetry job propagation",
  () => {
    let app: NestExpressApplication;
    let queue: Queue;

    async function finished(name: string): Promise<ReadableSpan> {
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const found = exporter
          .getFinishedSpans()
          .find((span) => span.name === name);
        if (found) {
          return found;
        }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      throw new Error(`No "${name}" span`);
    }

    beforeAll(async () => {
      api.context.setGlobalContextManager(
        new AsyncLocalStorageContextManager().enable(),
      );
      app = await NestFactory.create<NestExpressApplication>(QueueTestModule, {
        instrument: ObserveInstrument,
        logger: false,
      });
      await app.init();
      queue = app.get<Queue>(`BullQueue_${QUEUE_NAME}`);
    });

    afterAll(async () => {
      await queue?.obliterate({ force: true }).catch(() => undefined);
      await app?.close();
      api.context.disable();
    });

    it("runs the job in a trace of its own, linked to the request that enqueued it", async () => {
      await request(app.getHttpServer()).post("/signup").expect(201);

      const http = await finished("POST /signup");
      const run = await finished(`process ${QUEUE_NAME}`);
      const work = await finished("MailProcessor.process");

      expect(run.kind).toBe(api.SpanKind.CONSUMER);
      expect(run.parentSpanContext).toBeUndefined();
      expect(work.parentSpanContext?.spanId).toBe(run.spanContext().spanId);

      const traceId = http.spanContext().traceId;
      expect(run.spanContext().traceId).not.toBe(traceId);
      expect(run.links).toHaveLength(1);
      const [link] = run.links;
      expect(link.context.traceId).toBe(traceId);
      // Some span of the request - whichever step was current at `add`.
      const requestSpans = exporter
        .getFinishedSpans()
        .filter((span) => span.spanContext().traceId === traceId)
        .map((span) => span.spanContext().spanId);
      expect(requestSpans).toContain(link.context.spanId);
    });
  },
);
