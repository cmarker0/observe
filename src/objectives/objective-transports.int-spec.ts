import {
  Controller,
  Get,
  INestApplication,
  INestMicroservice,
  Module,
  Param,
  Post,
  VersioningType,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import {
  ClientProxy,
  ClientProxyFactory,
  MessagePattern,
  Payload,
  Transport,
} from "@nestjs/microservices";
import {
  FastifyAdapter,
  NestFastifyApplication,
} from "@nestjs/platform-fastify";
import { WsAdapter } from "@nestjs/platform-ws";
import {
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
} from "@nestjs/websockets";
import { firstValueFrom } from "rxjs";
import { WebSocket } from "ws";
import { ObserveOptions } from "../interfaces/observe-options.interface.js";
import { createObserveModule } from "../observe.module.js";
import {
  FakeCollector,
  WireSnapshot,
  startFakeCollector,
} from "../testing/fake-collector.js";
import { freePort, waitFor } from "../testing/observe-harness.js";
import { Objective } from "./objective.decorator.js";

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

/** The first snapshot to reach `collector` whose operation matches. */
async function deliveredSnapshot(
  collector: FakeCollector,
  matches: (snapshot: WireSnapshot) => boolean,
): Promise<WireSnapshot> {
  const find = () =>
    collector.batches.flatMap((batch) => batch.snapshots ?? []).find(matches);
  await waitFor(() => find() !== undefined, 8_000, "the snapshot to arrive");
  return find()!;
}

@Controller({ path: "orders", version: "1" })
class FastifyOrdersController {
  @Get(":id")
  @Objective({ availability: 99.9, latency: { underMs: 300, target: 99 } })
  findOne(@Param("id") id: string) {
    return { id };
  }

  @Post()
  @Objective({ availability: 99.5 })
  create() {
    throw new Error("deliberate");
  }
}

/**
 * The declaration's route comes from the adapter's route hook, and Fastify
 * implements that hook itself - so only a Fastify app shows its declarations
 * name the same template, with the same method, as Express's do.
 */
describe("@Objective on Fastify", () => {
  let app: NestFastifyApplication;
  let collector: FakeCollector;
  let baseUrl: string;
  let sent = 0;

  const send = async (method: string, path: string) => {
    const traceId = `fastify-${++sent}`;
    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { "x-request-id": traceId },
    });
    await response.arrayBuffer();
    return { traceId, status: response.status };
  };

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [
        ObserveModule.forRoot(observeOptions(collector, "fastify-app")),
      ],
      controllers: [FastifyOrdersController],
    })
    class FastifyObjectivesModule {}

    app = await NestFactory.create<NestFastifyApplication>(
      FastifyObjectivesModule,
      new FastifyAdapter(),
      { instrument: ObserveInstrument, logger: false },
    );
    app.setGlobalPrefix("api");
    app.enableVersioning({ type: VersioningType.URI });
    await app.listen(0, "127.0.0.1");
    baseUrl = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
    await collector?.close();
  });

  it("declares the route template Fastify served, with its method, once", async () => {
    const lookups = await Promise.all([
      send("GET", "/api/v1/orders/42"),
      send("GET", "/api/v1/orders/43"),
    ]);
    expect(lookups.map(({ status }) => status)).toEqual([200, 200]);
    await Promise.all(
      lookups.map(({ traceId }) => collector.waitForTrace(traceId)),
    );

    expect(
      collector
        .declarations()
        .filter((d) => d.handler === "FastifyOrdersController.findOne"),
    ).toEqual([
      {
        handler: "FastifyOrdersController.findOne",
        operationId: "/api/v1/orders/:id",
        method: "GET",
        objectives: [
          { availability: 99.9, latency: { underMs: 300, target: 99 } },
        ],
      },
    ]);
  });

  it("declares a route whose handler threw", async () => {
    const { traceId, status } = await send("POST", "/api/v1/orders");
    expect(status).toBe(500);
    const batch = await collector.waitForTrace(traceId);

    expect(batch.objectives).toEqual(
      expect.arrayContaining([
        {
          handler: "FastifyOrdersController.create",
          operationId: "/api/v1/orders",
          method: "POST",
          objectives: [{ availability: 99.5 }],
        },
      ]),
    );
    expect(collector.violations).toEqual([]);
  });
});

@Controller()
class PaymentsMessageController {
  @MessagePattern({ cmd: "charge" })
  @Objective({ availability: 99.9 })
  charge(@Payload() amount: number) {
    return { charged: amount };
  }
}

/**
 * A microservice's controllers are controllers too, so the registry reads
 * their `@Objective`s at boot. A message has a pattern rather than a route and
 * no method, and this shows what that makes of a declaration - and that a
 * process with no HTTP adapter at all still boots, answers, and reports.
 */
describe("@Objective on a microservice message handler", () => {
  let app: INestMicroservice;
  let client: ClientProxy;
  let collector: FakeCollector;

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [
        ObserveModule.forRoot(observeOptions(collector, "microservice-app")),
      ],
      controllers: [PaymentsMessageController],
    })
    class PaymentsModule {}

    const port = await freePort();
    app = await NestFactory.createMicroservice(PaymentsModule, {
      transport: Transport.TCP,
      options: { host: "127.0.0.1", port },
      instrument: ObserveInstrument,
      logger: false,
    } as never);
    await app.listen();

    client = ClientProxyFactory.create({
      transport: Transport.TCP,
      options: { host: "127.0.0.1", port },
    });
    await client.connect();
  });

  afterAll(async () => {
    await client?.close();
    await app?.close();
    await collector?.close();
  });

  it("keeps answering, and declares the pattern as the operation, with no method", async () => {
    await expect(
      firstValueFrom(client.send({ cmd: "charge" }, 42)),
    ).resolves.toEqual({ charged: 42 });

    const snapshot = await deliveredSnapshot(collector, (s) =>
      String(s.op).includes("charge"),
    );

    expect(collector.declarations()).toEqual([
      {
        handler: "PaymentsMessageController.charge",
        operationId: snapshot.op,
        objectives: [{ availability: 99.9 }],
      },
    ]);
    expect(collector.violations).toEqual([]);
  });
});

@WebSocketGateway()
class ChatGateway {
  @SubscribeMessage("join")
  @Objective({ availability: 99.9 })
  join(@MessageBody() room: string) {
    return { event: "joined", data: room };
  }
}

/**
 * Declarations are read off controllers, and a gateway is a provider - so an
 * `@Objective` on one of its handlers declares nothing. What matters is that
 * it costs nothing either: the socket still answers and its messages are
 * still reported.
 */
describe("@Objective on a WebSocket gateway handler", () => {
  let app: INestApplication;
  let socket: WebSocket;
  let collector: FakeCollector;

  beforeAll(async () => {
    collector = await startFakeCollector();
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [ObserveModule.forRoot(observeOptions(collector, "ws-app"))],
      providers: [ChatGateway],
    })
    class ChatModule {}

    app = await NestFactory.create(ChatModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    app.useWebSocketAdapter(new WsAdapter(app));
    const port = await freePort();
    await app.listen(port);

    socket = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
  });

  afterAll(async () => {
    socket?.close();
    await app?.close();
    await collector?.close();
  });

  it("declares nothing, and the gateway keeps answering and reporting", async () => {
    const reply = new Promise((resolve, reject) =>
      socket.once("message", (raw) =>
        Buffer.isBuffer(raw)
          ? resolve(JSON.parse(raw.toString("utf8")))
          : reject(new Error("Expected a text frame.")),
      ),
    );
    socket.send(JSON.stringify({ event: "join", data: "lobby" }));

    expect(await reply).toEqual({ event: "joined", data: "lobby" });
    await deliveredSnapshot(collector, (s) => s.op === "ChatGateway:join");

    for (const batch of collector.batches) {
      expect(batch).not.toHaveProperty("objectives");
    }
    expect(collector.violations).toEqual([]);
  });
});
