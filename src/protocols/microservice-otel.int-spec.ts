import { AsyncResource } from "node:async_hooks";
import { connect } from "node:net";
import {
  BadRequestException,
  Controller,
  Get,
  Inject,
  INestApplication,
  INestMicroservice,
  Injectable,
  IntrinsicException,
  Module,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import {
  BaseRpcContext,
  ClientProxy,
  ClientProxyFactory,
  ClientsModule,
  CustomTransportStrategy,
  EventPattern,
  MessagePattern,
  Payload,
  ReadPacket,
  Server,
  Transport,
  WritePacket,
} from "@nestjs/microservices";
import * as api from "@opentelemetry/api";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { firstValueFrom } from "rxjs";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import { freePort } from "../testing/observe-harness.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  parentIdOf,
  spanIdOf,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const REDIS_HOST = process.env.REDIS_HOST ?? "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);

/** The Redis transport needs a broker, so its suite runs only where one answers. */
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
 * Redis channels are shared with whatever else uses the broker, so this run's
 * patterns carry a suffix of their own.
 */
const CHANNEL = `${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
const REDIS_SUM = `observe.otel.sum.${CHANNEL}`;
const REDIS_CREATED = `observe.otel.created.${CHANNEL}`;

/**
 * Whether the installed `@nestjs/microservices` carries packet metadata over
 * its own transports - see `microservice-trace-propagation.int-spec.ts`.
 */
const frameworkCarriesMetadata =
  typeof (ClientProxy.prototype as { setOnDispatchHook?: unknown })
    .setOnDispatchHook === "function";

/** A caller's context, as an upstream service would have sent it. */
const CALLER_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN_ID = "00f067aa0ba902b7";
const CALLER_TRACEPARENT = `00-${CALLER_TRACE_ID}-${CALLER_SPAN_ID}-01`;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * One provider for every app in the file: the services and the caller run in
 * one process, so a trace that crosses a transport lands in one exporter and
 * can be asserted as one tree.
 */
const spans = new OtelTestSpans();
const mathObserve = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});
const callerObserve = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

// Bound at import time, because the client module is configured by a
// decorator that is evaluated then.
const TCP_PORT = await freePort();

/**
 * See `microservice-tcp-collection.int-spec.ts`: a failure modelled without
 * any HTTP exception underneath.
 */
class DeclinedException extends IntrinsicException {}

@Injectable()
class MathService {
  add(values: number[]): number {
    return (values ?? []).reduce((total, value) => total + value, 0);
  }
}

@Controller()
class MathController {
  constructor(private readonly math: MathService) {}

  @MessagePattern({ cmd: "sum" })
  sum(@Payload() values: number[]) {
    return this.math.add(values);
  }

  @MessagePattern("orders.find")
  find(@Payload() id: string) {
    return { id };
  }

  @MessagePattern({ cmd: "explode" })
  explode(): never {
    throw new Error("deliberate password=hunter2");
  }

  @MessagePattern({ cmd: "reject" })
  reject(): never {
    throw new BadRequestException("rejected");
  }

  @MessagePattern({ cmd: "decline" })
  decline(): never {
    throw new DeclinedException("declined");
  }

  @EventPattern("orders.created")
  // Declared for the decorator's sake - see the TCP collection suite.
  created(_event: unknown) {}

  @MessagePattern(REDIS_SUM)
  sumOverRedis(@Payload() values: number[]) {
    return this.math.add(values);
  }

  @EventPattern(REDIS_CREATED)
  createdOverRedis(_event: unknown) {}
}

@Module({
  imports: [mathObserve.ObserveModule.forRoot({})],
  controllers: [MathController],
  providers: [MathService],
})
class MathModule {}

/** The wire format of the loopback transport: a packet that has metadata. */
type LoopbackPacket = ReadPacket & {
  id?: string;
  metadata?: Record<string, string>;
};

/**
 * What `TcpContext` becomes on a framework release that carries packet
 * metadata: the pattern, and what the client attached.
 */
class LoopbackContext extends BaseRpcContext<[string, Record<string, string>]> {
  getPattern(): string {
    return this.args[0];
  }

  getMetadata(): Record<string, string> {
    return this.args[1];
  }
}

/**
 * An in-process transport that carries packet metadata, as Nest's own
 * transports do from the release that added `setOnDispatchHook`. The
 * installed release may predate that, and the propagation the module wires
 * up - the dispatch hook writing the caller's context, the server reading it
 * back off `getMetadata()` - deserves an end-to-end trace either way.
 *
 * Packets go through JSON, as over a wire, and are handled in the async
 * scope the server listened in, as a socket's data events are - so nothing
 * reaches the handler through the caller's async context, only through the
 * packet.
 */
class LoopbackServer extends Server implements CustomTransportStrategy {
  transportId = Symbol("LOOPBACK");
  private scope: AsyncResource | undefined;

  listen(callback: () => void) {
    this.scope = new AsyncResource("loopback.server");
    callback();
  }

  close() {}

  on() {}

  unwrap(): never {
    throw new Error("The loopback transport has nothing to unwrap.");
  }

  deliver(wire: string, reply: (packet: WritePacket) => void) {
    this.scope!.runInAsyncScope(() =>
      setImmediate(() => void this.receive(wire, reply)),
    );
  }

  private async receive(wire: string, reply: (packet: WritePacket) => void) {
    const packet = JSON.parse(wire) as LoopbackPacket;
    const pattern = this.normalizePattern(packet.pattern);
    const context = new LoopbackContext([pattern, packet.metadata ?? {}]);
    if (packet.id === undefined) {
      return this.handleEvent(pattern, packet, context);
    }
    const handler = this.getHandlerByPattern(pattern)!;
    return this.onProcessingStartHook(this.transportId, context, async () => {
      const response$ = this.transformToObservable(
        await handler(packet.data, context),
      );
      this.send(response$, (response) => {
        this.onProcessingEndHook?.(this.transportId, context);
        reply({ ...response, id: packet.id } as WritePacket);
      });
    });
  }
}

const loopbackServer = new LoopbackServer();

/**
 * The client side of the loopback transport, shaped like Nest's `ClientProxy`
 * from the release that added the dispatch hook: the hook is kept in a
 * protected `onDispatchHook` field and run from `createPacket`, against every
 * packet on its way out.
 *
 * `presetMetadata` stands in for an application that stamps packets itself.
 */
class LoopbackClient extends ClientProxy {
  protected onDispatchHook?: (packet: LoopbackPacket) => void;

  constructor(private readonly presetMetadata?: Record<string, string>) {
    super();
  }

  setOnDispatchHook(hook: (packet: LoopbackPacket) => void) {
    this.onDispatchHook = hook;
  }

  async connect() {}

  close() {}

  unwrap(): never {
    throw new Error("The loopback transport has nothing to unwrap.");
  }

  protected createPacket(packet: ReadPacket): LoopbackPacket {
    const out: LoopbackPacket = this.presetMetadata
      ? { ...packet, metadata: { ...this.presetMetadata } }
      : { ...packet };
    this.onDispatchHook?.(out);
    return out;
  }

  protected publish(
    packet: ReadPacket,
    callback: (packet: WritePacket) => void,
  ) {
    const wire = JSON.stringify(this.assignPacketId(this.createPacket(packet)));
    // The reply lands in the caller's scope, as a client socket's data does.
    const scope = new AsyncResource("loopback.reply");
    loopbackServer.deliver(wire, (reply) =>
      scope.runInAsyncScope(() => callback(reply)),
    );
    return () => undefined;
  }

  protected async dispatchEvent<T>(packet: ReadPacket): Promise<T> {
    loopbackServer.deliver(JSON.stringify(this.createPacket(packet)), () => {});
    return undefined as T;
  }
}

@Controller()
class ApiController {
  constructor(
    @Inject("TCP") private readonly tcp: ClientProxy,
    @Inject("LOOPBACK") private readonly loopback: ClientProxy,
    @Inject("LOOPBACK_PRESET") private readonly preset: ClientProxy,
  ) {}

  @Get("tcp")
  async overTcp() {
    return { total: await firstValueFrom(this.tcp.send({ cmd: "sum" }, [1])) };
  }

  @Get("loopback")
  async overLoopback() {
    return {
      total: await firstValueFrom(this.loopback.send({ cmd: "sum" }, [1, 2])),
    };
  }

  @Get("loopback-event")
  async eventOverLoopback() {
    await firstValueFrom(this.loopback.emit("orders.created", { id: 1 }), {
      defaultValue: undefined,
    });
    return { ok: true };
  }

  @Get("loopback-preset")
  async presetOverLoopback() {
    return {
      total: await firstValueFrom(this.preset.send({ cmd: "sum" }, [4])),
    };
  }
}

@Module({
  imports: [
    callerObserve.ObserveModule.forRoot({}),
    ClientsModule.register([
      {
        name: "TCP",
        transport: Transport.TCP,
        options: { host: "127.0.0.1", port: TCP_PORT },
      },
    ]),
  ],
  controllers: [ApiController],
  providers: [
    { provide: "LOOPBACK", useFactory: () => new LoopbackClient() },
    {
      provide: "LOOPBACK_PRESET",
      useFactory: () =>
        new LoopbackClient({
          traceparent: CALLER_TRACEPARENT,
          "x-request-id": "caller-chose-this",
        }),
    },
  ],
})
class ApiModule {}

/** The microservice's SERVER (or CONSUMER) span: the one named `name`. */
const operationNamed = (name: string) =>
  spans.waitFor(
    (finished) =>
      finished.find(
        (span) => span.name === name && span.kind !== api.SpanKind.INTERNAL,
      ),
    3000,
    `a "${name}" operation span`,
  );

/** The finished spans of `root`'s trace. */
const traceOfSpan = (root: ReadableSpan) =>
  spans.finished.filter(
    (span) => span.spanContext().traceId === root.spanContext().traceId,
  );

/** `root` and everything beneath it - one service's part of a longer trace. */
function subtreeOf(trace: ReadableSpan[], root: ReadableSpan): ReadableSpan[] {
  const below = new Set([spanIdOf(root)]);
  let grew = true;
  while (grew) {
    grew = false;
    for (const span of trace) {
      const parent = parentIdOf(span);
      if (parent && below.has(parent) && !below.has(spanIdOf(span))) {
        below.add(spanIdOf(span));
        grew = true;
      }
    }
  }
  return trace.filter((span) => below.has(spanIdOf(span)));
}

/** The names from `span` up to its trace's root, nearest first. */
function ancestryOf(trace: ReadableSpan[], span: ReadableSpan): string[] {
  const byId = new Map(trace.map((each) => [spanIdOf(each), each]));
  const names: string[] = [];
  let cursor = byId.get(parentIdOf(span) ?? "");
  while (cursor) {
    names.push(cursor.name);
    cursor = byId.get(parentIdOf(cursor) ?? "");
  }
  return names;
}

beforeAll(() => installOtelGlobals());
afterAll(() => uninstallOtelGlobals());

/**
 * `microservice-tcp-collection.int-spec.ts` with OpenTelemetry recording: each
 * message becomes a SERVER span named after its pattern, with the handler and
 * the providers it calls beneath it.
 *
 * TCP is a request transport rather than a message broker, so its operations
 * are RPC-shaped - `rpc.system` and `rpc.method` - whether the client sent a
 * message or an event. Brokers are covered by the Redis suite below.
 */
describe("ObserveModule with OpenTelemetry: microservice (TCP)", () => {
  let app: INestMicroservice;
  let client: ClientProxy;

  beforeAll(async () => {
    const port = await freePort();
    app = await NestFactory.createMicroservice(MathModule, {
      transport: Transport.TCP,
      options: { host: "127.0.0.1", port },
      instrument: mathObserve.ObserveInstrument,
      logger: false,
    } as never);
    await app.listen();

    // A bare client outside any Nest app: nothing on the caller's side is
    // recorded, so every span here is the server's.
    client = ClientProxyFactory.create({
      transport: Transport.TCP,
      options: { host: "127.0.0.1", port },
    });
    await client.connect();
  });

  afterAll(async () => {
    await client?.close();
    await app?.close();
  });

  beforeEach(() => spans.reset());

  it("records a message as a SERVER span over its handler and providers", async () => {
    await expect(
      firstValueFrom(client.send<number>({ cmd: "sum" }, [1, 2, 3])),
    ).resolves.toBe(6);

    const trace = await spans.traceOf('{"cmd":"sum"}');
    expect(spanTree(trace)).toBe(
      [
        'SERVER {"cmd":"sum"}',
        "  INTERNAL MathController.sum",
        "    INTERNAL MathService.add",
      ].join("\n"),
    );

    const root = spanNamed(trace, '{"cmd":"sum"}');
    // A caller that sent no context starts a trace of its own.
    expect(root.parentSpanContext).toBeUndefined();
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    // Transport[Transport.TCP], by name, not the numeric enum.
    expect(root.attributes).toMatchObject({
      "rpc.system": "tcp",
      "rpc.method": '{"cmd":"sum"}',
      [ObserveAttributes.PROTOCOL]: "TCP",
      [ObserveAttributes.OPERATION_ID]: '{"cmd":"sum"}',
    });
    // Nothing on a TCP packet of this release to adopt, so the agent mints one.
    expect(root.attributes[ObserveAttributes.CORRELATION_ID]).toMatch(UUID);
  });

  it("starts one trace per message, named after each pattern", async () => {
    await firstValueFrom(client.send({ cmd: "sum" }, [1]));
    await firstValueFrom(client.send("orders.find", "abc"));
    await firstValueFrom(client.send({ cmd: "sum" }, [2]));

    // Otherwise every message in a service would aggregate into one operation.
    const roots = await spans.waitFor((finished) => {
      const found = finished.filter(
        (span) => span.kind === api.SpanKind.SERVER,
      );
      return found.length >= 3 ? found : undefined;
    });
    expect(roots.map((span) => span.name).sort()).toEqual([
      "orders.find",
      '{"cmd":"sum"}',
      '{"cmd":"sum"}',
    ]);
    expect(new Set(roots.map((span) => span.spanContext().traceId)).size).toBe(
      3,
    );
    expect(spanNamed(roots, "orders.find").attributes["rpc.method"]).toBe(
      "orders.find",
    );
  });

  /**
   * RPC has no response status to read, so the throw is the whole signal: an
   * unmodelled one is filed under 500 and fails the operation.
   */
  it("fails a message whose handler threw, with the error redacted", async () => {
    await expect(
      firstValueFrom(client.send({ cmd: "explode" }, {})),
    ).rejects.toBeDefined();

    const trace = await spans.traceOf('{"cmd":"explode"}');
    expect(spanTree(trace)).toBe(
      ['SERVER {"cmd":"explode"}', "  INTERNAL MathController.explode"].join(
        "\n",
      ),
    );

    const root = spanNamed(trace, '{"cmd":"explode"}');
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 500,
      [ObserveAttributes.ERROR_HANDLED]: false,
      "error.type": "Error",
    });

    const handler = spanNamed(trace, "MathController.explode");
    expect(handler.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(handler.events.map((event) => event.name)).toEqual(["exception"]);
    expect(handler.events[0].attributes?.["exception.message"]).toBe(
      "deliberate password=[REDACTED]",
    );
  });

  /**
   * A failure raised on purpose is filed under a 4xx and marked handled, so
   * the backend counts it apart from a crash. Off HTTP it still fails the
   * span: there is no client to blame it on.
   */
  it("marks a deliberately raised failure as handled", async () => {
    await expect(
      firstValueFrom(client.send({ cmd: "reject" }, {})),
    ).rejects.toBeDefined();

    const root = await operationNamed('{"cmd":"reject"}');
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 400,
      [ObserveAttributes.ERROR_HANDLED]: true,
      "error.type": "BadRequestException",
    });
  });

  it("marks a bare IntrinsicException as handled too", async () => {
    await expect(
      firstValueFrom(client.send({ cmd: "decline" }, {})),
    ).rejects.toBeDefined();

    // No `getStatus()` to read: the base class alone files it under 400.
    const root = await operationNamed('{"cmd":"decline"}');
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 400,
      [ObserveAttributes.ERROR_HANDLED]: true,
      "error.type": "DeclinedException",
    });
  });

  /**
   * Events send no response, so only the processing-end hook can end the
   * span. Over TCP the event is still RPC-shaped: the kind follows the
   * transport, not the pattern decorator.
   */
  it("records an event, which sends no response", async () => {
    client.emit("orders.created", { id: 1 });

    const trace = await spans.traceOf("orders.created");
    expect(spanTree(trace)).toBe(
      ["SERVER orders.created", "  INTERNAL MathController.created"].join("\n"),
    );
    expect(spanNamed(trace, "orders.created").attributes).toMatchObject({
      "rpc.system": "tcp",
      "rpc.method": "orders.created",
    });
  });
});

/**
 * A broker transport. Redis delivers messages rather than calls, so its
 * operations are CONSUMER spans named `process <channel>` with `messaging.*`
 * attributes - a request-response message as much as an event.
 */
describe.runIf(redisReachable)(
  "ObserveModule with OpenTelemetry: microservice (Redis)",
  () => {
    let app: INestMicroservice;
    let client: ClientProxy;

    beforeAll(async () => {
      const options = { host: REDIS_HOST, port: REDIS_PORT };
      app = await NestFactory.createMicroservice(MathModule, {
        transport: Transport.REDIS,
        options,
        instrument: mathObserve.ObserveInstrument,
        logger: false,
      } as never);
      await app.listen();

      client = ClientProxyFactory.create({
        transport: Transport.REDIS,
        options,
      });
      await client.connect();
    });

    afterAll(async () => {
      await client?.close();
      await app?.close();
    });

    beforeEach(() => spans.reset());

    it("records a message as a CONSUMER span over its handler and providers", async () => {
      await expect(
        firstValueFrom(client.send<number>(REDIS_SUM, [2, 3])),
      ).resolves.toBe(5);

      const trace = await spans.traceOf(`process ${REDIS_SUM}`);
      expect(spanTree(trace)).toBe(
        [
          `CONSUMER process ${REDIS_SUM}`,
          "  INTERNAL MathController.sumOverRedis",
          "    INTERNAL MathService.add",
        ].join("\n"),
      );
      expect(spanNamed(trace, `process ${REDIS_SUM}`).attributes).toMatchObject(
        {
          "messaging.system": "redis",
          "messaging.operation.type": "process",
          "messaging.destination.name": REDIS_SUM,
          [ObserveAttributes.PROTOCOL]: "REDIS",
        },
      );
    });

    it("records an event as a CONSUMER span too", async () => {
      client.emit(REDIS_CREATED, { id: 1 });

      const trace = await spans.traceOf(`process ${REDIS_CREATED}`);
      expect(spanTree(trace)).toBe(
        [
          `CONSUMER process ${REDIS_CREATED}`,
          "  INTERNAL MathController.createdOverRedis",
        ].join("\n"),
      );
      expect(
        spanNamed(trace, `process ${REDIS_CREATED}`).attributes,
      ).toMatchObject({
        "messaging.system": "redis",
        "messaging.destination.name": REDIS_CREATED,
      });
    });
  },
);

/**
 * `microservice-trace-propagation.int-spec.ts` with OpenTelemetry: an HTTP
 * request that calls a microservice through an injected `ClientProxy`. The
 * module's dispatch hook injects the caller's span context into the packet
 * metadata; the server's agent extracts it, so the microservice's SERVER span
 * continues the caller's trace.
 */
describe("ObserveModule with OpenTelemetry: a trace from HTTP into a microservice", () => {
  let apiApp: INestApplication;
  let tcpApp: INestMicroservice;
  let loopbackApp: INestMicroservice;

  beforeAll(async () => {
    tcpApp = await NestFactory.createMicroservice(MathModule, {
      transport: Transport.TCP,
      options: { host: "127.0.0.1", port: TCP_PORT },
      instrument: mathObserve.ObserveInstrument,
      logger: false,
    } as never);
    await tcpApp.listen();

    loopbackApp = await NestFactory.createMicroservice(MathModule, {
      strategy: loopbackServer,
      instrument: mathObserve.ObserveInstrument,
      logger: false,
    } as never);
    await loopbackApp.listen();

    apiApp = await NestFactory.create(ApiModule, {
      instrument: callerObserve.ObserveInstrument,
      logger: false,
    });
    await apiApp.init();
  });

  afterAll(async () => {
    await apiApp?.close();
    await loopbackApp?.close();
    await tcpApp?.close();
  });

  beforeEach(() => spans.reset());

  it("continues the caller's trace in the microservice, under the call that sent it", async () => {
    const response = await request(apiApp.getHttpServer())
      .get("/loopback")
      .expect(200);
    expect(response.body).toEqual({ total: 3 });

    const server = await operationNamed('{"cmd":"sum"}');
    const http = await operationNamed("GET /loopback");
    const trace = traceOfSpan(http);

    expect(server.kind).toBe(api.SpanKind.SERVER);
    expect(server.spanContext().traceId).toBe(http.spanContext().traceId);
    // The microservice's own part of the trace, as it would be on its own.
    expect(spanTree(subtreeOf(trace, server))).toBe(
      [
        'SERVER {"cmd":"sum"}',
        "  INTERNAL MathController.sum",
        "    INTERNAL MathService.add",
      ].join("\n"),
    );
    // Hung straight from the caller's handler: the client's own plumbing
    // (`createPacket`, `publish`, `connect`) is not application code.
    expect(ancestryOf(trace, server)).toEqual([
      "ApiController.overLoopback",
      "GET /loopback",
    ]);
    expect(
      trace.filter((span) => span.name.startsWith("LoopbackClient.")),
    ).toEqual([]);
    expect(server.attributes).toMatchObject({
      "rpc.system": "loopback",
      [ObserveAttributes.PROTOCOL]: "LOOPBACK",
    });
  });

  /**
   * The dispatch hook still stamps `x-request-id`, from the async store's
   * trace id key. In OpenTelemetry mode that key holds the OTel trace id (see
   * `OtelSpanRecorder.correlate`), so the microservice correlates on the
   * trace id, not on the request id the edge sent.
   */
  it("hands the microservice the trace id to correlate on", async () => {
    await request(apiApp.getHttpServer())
      .get("/loopback")
      .set("x-request-id", "edge-7")
      .expect(200);

    const server = await operationNamed('{"cmd":"sum"}');
    const http = await operationNamed("GET /loopback");
    expect(http.attributes[ObserveAttributes.CORRELATION_ID]).toBe("edge-7");
    expect(server.attributes[ObserveAttributes.CORRELATION_ID]).toBe(
      http.spanContext().traceId,
    );
  });

  it("continues the trace through an event as well", async () => {
    await request(apiApp.getHttpServer()).get("/loopback-event").expect(200);

    const server = await operationNamed("orders.created");
    const http = await operationNamed("GET /loopback-event");
    const trace = traceOfSpan(http);
    expect(server.spanContext().traceId).toBe(http.spanContext().traceId);
    expect(spanTree(subtreeOf(trace, server))).toBe(
      ["SERVER orders.created", "  INTERNAL MathController.created"].join("\n"),
    );
    expect(ancestryOf(trace, server)).toContain(
      "ApiController.eventOverLoopback",
    );
  });

  /**
   * The hook only fills in what is missing: a `traceparent` or `x-request-id`
   * the application put on the packet itself is what the microservice sees.
   */
  it("leaves a traceparent and request id the caller set on the packet alone", async () => {
    await request(apiApp.getHttpServer()).get("/loopback-preset").expect(200);

    const server = await operationNamed('{"cmd":"sum"}');
    expect(server.spanContext().traceId).toBe(CALLER_TRACE_ID);
    expect(parentIdOf(server)).toBe(CALLER_SPAN_ID);
    expect(server.attributes[ObserveAttributes.CORRELATION_ID]).toBe(
      "caller-chose-this",
    );
    const http = await operationNamed("GET /loopback-preset");
    expect(http.spanContext().traceId).not.toBe(CALLER_TRACE_ID);
  });

  /**
   * The instance decorator wraps every function-valued property of a
   * provider, and Nest keeps the agent's dispatch hook in
   * `ClientProxy#onDispatchHook`, calling it as `this.onDispatchHook(...)`.
   * Wrapped, the hook would run inside a `<Client>.onDispatchHook` span,
   * inject *that* span's context, and hang the microservice's SERVER span
   * from the agent's own bookkeeping. The hook is marked to be left alone.
   */
  it("does not record the agent's dispatch hook as an application span", async () => {
    await request(apiApp.getHttpServer()).get("/loopback").expect(200);

    const server = await operationNamed('{"cmd":"sum"}');
    const trace = traceOfSpan(server);
    expect(trace.map((span) => span.name)).not.toContain(
      "LoopbackClient.onDispatchHook",
    );
    expect(ancestryOf(trace, server)[0]).toBe("ApiController.overLoopback");
  });

  it.runIf(frameworkCarriesMetadata)(
    "continues the caller's trace over Nest's own TCP transport",
    async () => {
      await request(apiApp.getHttpServer()).get("/tcp").expect(200);

      const server = await operationNamed('{"cmd":"sum"}');
      const http = await operationNamed("GET /tcp");
      const trace = traceOfSpan(http);
      expect(server.spanContext().traceId).toBe(http.spanContext().traceId);
      expect(ancestryOf(trace, server)).toContain("ApiController.overTcp");
    },
  );

  it.runIf(!frameworkCarriesMetadata)(
    "starts a trace of its own over a TCP transport that carries no metadata, and the call still goes through",
    async () => {
      const response = await request(apiApp.getHttpServer())
        .get("/tcp")
        .expect(200);
      expect(response.body).toEqual({ total: 1 });

      const server = await operationNamed('{"cmd":"sum"}');
      const http = await operationNamed("GET /tcp");
      expect(server.spanContext().traceId).not.toBe(http.spanContext().traceId);
      expect(server.parentSpanContext).toBeUndefined();
      expect(spanTree(traceOfSpan(server))).toBe(
        [
          'SERVER {"cmd":"sum"}',
          "  INTERNAL MathController.sum",
          "    INTERNAL MathService.add",
        ].join("\n"),
      );
      expect(http.status.code).toBe(api.SpanStatusCode.UNSET);
    },
  );
});
