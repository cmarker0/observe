import {
  CanActivate,
  ConflictException,
  ExecutionContext,
  INestApplication,
  Injectable,
  Module,
  ParseIntPipe,
  UseGuards,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { IoAdapter } from "@nestjs/platform-socket.io";
import { WsAdapter } from "@nestjs/platform-ws";
import {
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
  WsException,
} from "@nestjs/websockets";
import * as api from "@opentelemetry/api";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { createServer, Server } from "http";
import { AddressInfo } from "net";
import { from, map } from "rxjs";
import { io } from "socket.io-client";
import { WebSocket } from "ws";
import { ObserveOptions } from "../interfaces/observe-options.interface.js";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import { TracerService } from "../services/tracer.service.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let downstreamUrl = "";

@Injectable()
class RoomsService {
  join(room: string) {
    return `joined:${room}`;
  }

  async seat(room: string) {
    await sleep(5);
    return `seated:${room}`;
  }
}

@Injectable()
class RatesService {
  async lookup() {
    const response = await fetch(`${downstreamUrl}/rates`);
    await response.text();
    return "looked-up";
  }
}

@Injectable()
class MembersOnlyGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    if (context.switchToWs().getData() === "member") {
      return true;
    }
    throw new WsException("members only");
  }
}

@WebSocketGateway()
class DeskGateway {
  constructor(
    private readonly rooms: RoomsService,
    private readonly rates: RatesService,
    private readonly tracer: TracerService,
  ) {}

  @SubscribeMessage("join")
  join(@MessageBody() room: string) {
    return { event: "joined", data: this.rooms.join(room) };
  }

  /** Calls a provider, yields, and calls more - the interleaving case. */
  @SubscribeMessage("visit")
  async visit(@MessageBody() body: { room: string; delayMs: number }) {
    this.rooms.join(body.room);
    await sleep(body.delayMs);
    return { event: "visited", data: await this.rooms.seat(body.room) };
  }

  // A plain return value: socket.io answers it through the acknowledgement.
  @SubscribeMessage("count")
  count(@MessageBody() room: string) {
    return this.rooms.join(room).length;
  }

  @SubscribeMessage("ping")
  ping() {
    return { event: "pong", data: this.rooms.join("ping") };
  }

  @SubscribeMessage("explode")
  explode() {
    throw new Error("deliberate password=hunter2");
  }

  @SubscribeMessage("decline")
  decline() {
    throw new ConflictException("already seated");
  }

  @SubscribeMessage("reject-later")
  async rejectLater() {
    await sleep(20);
    throw new Error("rejected after a wait");
  }

  @SubscribeMessage("later")
  async later() {
    await sleep(60);
    return { event: "later-done", data: "slept" };
  }

  @SubscribeMessage("ticks")
  ticks() {
    return from([1, 2, 3]).pipe(map((tick) => ({ event: "tick", data: tick })));
  }

  @SubscribeMessage("vault")
  @UseGuards(MembersOnlyGuard)
  vault() {
    return { event: "vault-open", data: true };
  }

  @SubscribeMessage("square")
  square(@MessageBody(ParseIntPipe) value: number) {
    return { event: "squared", data: value * value };
  }

  @SubscribeMessage("lookup")
  async lookup() {
    return { event: "looked-up", data: await this.rates.lookup() };
  }

  @SubscribeMessage("whoami")
  whoami() {
    return { event: "you-are", data: this.tracer.currentTraceId() };
  }

  /** Reads the trace id only after yielding, while other messages run. */
  @SubscribeMessage("whoami-later")
  async whoamiLater(@MessageBody() delayMs: number) {
    await sleep(delayMs);
    return { event: "you-were", data: this.tracer.currentTraceId() };
  }

  @SubscribeMessage("sampled-out")
  sampledOut() {
    return { event: "sampled-out-done", data: this.rooms.join("nobody") };
  }
}

type Platform = "ws" | "socket.io";

/** One client API over both transports: send a message, await a named reply. */
interface TestClient {
  send(event: string, data?: unknown): void;
  next(event: string): Promise<unknown>;
  collect(event: string): unknown[];
  ack?(event: string, data?: unknown): Promise<unknown>;
  close(): void;
}

async function connectClient(
  platform: Platform,
  port: number,
): Promise<TestClient> {
  if (platform === "ws") {
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    const listeners = new Set<(event: string, data: unknown) => void>();
    socket.on("message", (raw) => {
      const { event, data } = JSON.parse((raw as Buffer).toString("utf8"));
      for (const listener of listeners) {
        listener(event, data);
      }
    });
    return {
      send: (event, data) => socket.send(JSON.stringify({ event, data })),
      next: (event) =>
        new Promise((resolve) => {
          const listener = (received: string, data: unknown) => {
            if (received === event) {
              listeners.delete(listener);
              resolve(data);
            }
          };
          listeners.add(listener);
        }),
      collect: (event) => {
        const seen: unknown[] = [];
        listeners.add((received, data) => {
          if (received === event) {
            seen.push(data);
          }
        });
        return seen;
      },
      close: () => socket.close(),
    };
  }

  const socket = io(`http://127.0.0.1:${port}`);
  await new Promise((resolve, reject) => {
    socket.once("connect", () => resolve(undefined));
    socket.once("connect_error", reject);
  });
  return {
    send: (event, data) => void socket.emit(event, data),
    next: (event) => new Promise((resolve) => socket.once(event, resolve)),
    collect: (event) => {
      const seen: unknown[] = [];
      socket.on(event, (data: unknown) => seen.push(data));
      return seen;
    },
    ack: (event, data) => socket.emitWithAck(event, data),
    close: () => void socket.close(),
  };
}

async function bootApp(platform: Platform, ws: ObserveOptions["ws"]) {
  const spans = new OtelTestSpans();
  const { ObserveModule, ObserveInstrument } = createObserveModule({
    opentelemetry: { tracerProvider: spans.provider },
  });

  @Module({
    imports: [
      ObserveModule.forRoot({
        ws,
        tracesSampleRate: (protocol, attributes) =>
          !(protocol === "ws" && attributes?.pattern === "sampled-out"),
      }),
    ],
    providers: [RoomsService, RatesService, MembersOnlyGuard, DeskGateway],
  })
  class WsOtelTestModule {}

  const app = await NestFactory.create(WsOtelTestModule, {
    instrument: ObserveInstrument,
    logger: false,
  });
  app.useWebSocketAdapter(
    platform === "ws" ? new WsAdapter(app) : new IoAdapter(app),
  );
  await app.listen(0, "127.0.0.1");
  const { port } = app.getHttpServer().address() as AddressInfo;
  return { app, spans, port, client: await connectClient(platform, port) };
}

/** Every finished span, grouped by trace and drawn as a tree per trace. */
function treesByTrace(finished: ReadableSpan[]): Map<string, string> {
  const traces = new Map<string, ReadableSpan[]>();
  for (const span of finished) {
    const { traceId } = span.spanContext();
    traces.set(traceId, [...(traces.get(traceId) ?? []), span]);
  }
  return new Map(
    [...traces].map(([traceId, members]) => [traceId, spanTree(members)]),
  );
}

const millis = ([seconds, nanos]: api.HrTime) => seconds * 1e3 + nanos / 1e6;

/**
 * Gateway messages recorded as OpenTelemetry spans, on both platform
 * adapters - the counterpart of `ws-collection.int-spec.ts`,
 * `ws-socketio-collection.int-spec.ts` and `ws-scenarios.int-spec.ts`.
 *
 * Each message is an operation of its own: a SERVER span named
 * `Gateway:pattern`, in a new trace, carrying `nestjs.observe.protocol: ws`
 * and no `rpc.*` attributes. Beneath it, the chain `WsContextCreator.create`
 * built - guards, pipes, the handler - and the providers the handler reaches.
 *
 * Nest's ws proxy hands a thrown error to the exception filter and resolves,
 * so the operation learns of a failure only from the step directly under it.
 * Unlike HTTP, a gateway message has no client-error class: any error that
 * escaped the chain fails the span, handled or not.
 */
describe.each<Platform>(["ws", "socket.io"])(
  "ObserveModule: WebSocket gateway over OpenTelemetry (%s)",
  (platform) => {
    let downstream: Server;

    beforeAll(async () => {
      installOtelGlobals();
      downstream = createServer((_req, res) => {
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ ok: true }));
      });
      await new Promise<void>((resolve) =>
        downstream.listen(0, "127.0.0.1", resolve),
      );
      const { port } = downstream.address() as AddressInfo;
      downstreamUrl = `http://127.0.0.1:${port}`;
    });

    afterAll(async () => {
      downstream.closeAllConnections();
      await new Promise((resolve) => downstream.close(resolve));
      uninstallOtelGlobals();
    });

    describe("with tracing on", () => {
      let app: INestApplication;
      let spans: OtelTestSpans;
      let port: number;
      let client: TestClient;

      beforeAll(async () => {
        ({ app, spans, port, client } = await bootApp(platform, {
          tags: { environment: "test" },
          ignore: (message) => message.pattern === "ping",
          getUserId: (message) =>
            message.data === "lobby" ? "u-1" : undefined,
        }));
      });

      afterAll(async () => {
        client?.close();
        await app?.close();
      });

      beforeEach(() => spans.reset());

      const traceOf = (pattern: string) =>
        spans.traceOf(`DeskGateway:${pattern}`);

      it("records a message as a SERVER root over its handler and the providers it calls", async () => {
        const reply = client.next("joined");
        client.send("join", "lobby");

        // The wrapped handler still answers the client.
        expect(await reply).toBe("joined:lobby");

        const trace = await traceOf("join");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:join",
            "  INTERNAL DeskGateway.join",
            "    INTERNAL RoomsService.join",
          ].join("\n"),
        );
        const root = spanNamed(trace, "DeskGateway:join");
        expect(root.parentSpanContext).toBeUndefined();
        expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
        expect(root.attributes).toMatchObject({
          [ObserveAttributes.PROTOCOL]: "ws",
          [ObserveAttributes.OPERATION_ID]: "DeskGateway:join",
          [ObserveAttributes.CORRELATION_ID]: expect.any(String),
          environment: "test",
          "enduser.id": "u-1",
        });
        // A gateway message is neither RPC nor messaging.
        expect(
          Object.keys(root.attributes).filter(
            (key) => key.startsWith("rpc.") || key.startsWith("messaging."),
          ),
        ).toEqual([]);
      });

      it("fails the operation of a handler that throws, with the error redacted", async () => {
        const exception = client.next("exception");
        client.send("explode");
        // Nest's filter still tells the client.
        await exception;

        const trace = await traceOf("explode");
        expect(spanTree(trace)).toBe(
          ["SERVER DeskGateway:explode", "  INTERNAL DeskGateway.explode"].join(
            "\n",
          ),
        );
        const handler = spanNamed(trace, "DeskGateway.explode");
        expect(handler.status.code).toBe(api.SpanStatusCode.ERROR);
        expect(handler.events.map((event) => event.name)).toEqual([
          "exception",
        ]);
        expect(JSON.stringify(trace.map((span) => span.events))).not.toContain(
          "hunter2",
        );

        const root = spanNamed(trace, "DeskGateway:explode");
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
        expect(root.attributes).toMatchObject({
          [ObserveAttributes.STATUS_CODE]: 500,
          [ObserveAttributes.ERROR_HANDLED]: false,
          "error.type": "Error",
        });
      });

      it("classifies a WsException as unhandled: it is not an IntrinsicException", async () => {
        const exception = client.next("exception");
        client.send("vault", "stranger");
        expect(await exception).toMatchObject({ message: "members only" });

        // The guard refused, so the handler never ran: the guard is the only
        // step, and its error is the operation's.
        const trace = await traceOf("vault");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:vault",
            "  INTERNAL MembersOnlyGuard.canActivate",
          ].join("\n"),
        );
        const root = spanNamed(trace, "DeskGateway:vault");
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
        expect(root.attributes).toMatchObject({
          [ObserveAttributes.STATUS_CODE]: 500,
          [ObserveAttributes.ERROR_HANDLED]: false,
          "error.type": "WsException",
        });
      });

      it("records a message a guard let through, guard first and handler after", async () => {
        const reply = client.next("vault-open");
        client.send("vault", "member");
        expect(await reply).toBe(true);

        const trace = await traceOf("vault");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:vault",
            "  INTERNAL MembersOnlyGuard.canActivate",
            "  INTERNAL DeskGateway.vault",
          ].join("\n"),
        );
        expect(spanNamed(trace, "DeskGateway:vault").status.code).toBe(
          api.SpanStatusCode.UNSET,
        );
      });

      it("classifies an intrinsic exception as handled, under its own 4xx, and still fails the span", async () => {
        client.send("decline");

        const trace = await traceOf("decline");
        const root = spanNamed(trace, "DeskGateway:decline");
        expect(root.attributes).toMatchObject({
          [ObserveAttributes.STATUS_CODE]: 409,
          [ObserveAttributes.ERROR_HANDLED]: true,
          "error.type": "ConflictException",
        });
        // No HTTP here, so no client-error carve-out.
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
      });

      it("fails the operation of a handler that rejects after a wait", async () => {
        client.send("reject-later");

        const trace = await traceOf("reject-later");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:reject-later",
            "  INTERNAL DeskGateway.rejectLater",
          ].join("\n"),
        );
        const root = spanNamed(trace, "DeskGateway:reject-later");
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
        expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
        expect(
          spanNamed(trace, "DeskGateway.rejectLater").events[0]?.attributes?.[
            "exception.message"
          ],
        ).toBe("rejected after a wait");
      });

      it("records a pipe that rejected the payload as the failing step, the handler never run", async () => {
        client.send("square", "not-a-number");
        const rejected = await traceOf("square");
        expect(spanTree(rejected)).toBe(
          [
            "SERVER DeskGateway:square",
            "  INTERNAL ParseIntPipe.transform",
            "    INTERNAL ParseIntPipe.isNumeric",
            "    INTERNAL ParseIntPipe.exceptionFactory",
          ].join("\n"),
        );
        expect(spanNamed(rejected, "DeskGateway:square").status.code).toBe(
          api.SpanStatusCode.ERROR,
        );

        spans.reset();
        const reply = client.next("squared");
        client.send("square", "7");
        expect(await reply).toBe(49);
        const accepted = await traceOf("square");
        expect(spanTree(accepted)).toBe(
          [
            "SERVER DeskGateway:square",
            "  INTERNAL ParseIntPipe.transform",
            "    INTERNAL ParseIntPipe.isNumeric",
            "  INTERNAL DeskGateway.square",
          ].join("\n"),
        );
        expect(spanNamed(accepted, "DeskGateway:square").status.code).toBe(
          api.SpanStatusCode.UNSET,
        );
      });

      it("keeps the root open for a handler that answers later", async () => {
        const reply = client.next("later-done");
        client.send("later");
        expect(await reply).toBe("slept");

        const trace = await traceOf("later");
        const root = spanNamed(trace, "DeskGateway:later");
        const handler = spanNamed(trace, "DeskGateway.later");
        expect(millis(handler.duration)).toBeGreaterThanOrEqual(50);
        expect(millis(root.duration)).toBeGreaterThanOrEqual(
          millis(handler.duration),
        );
      });

      it("records every emission of an observable handler under one root", async () => {
        const ticks = client.collect("tick");
        client.send("ticks");

        await spans.waitFor(() => (ticks.length === 3 ? true : undefined));
        expect(ticks).toEqual([1, 2, 3]);
        const trace = await traceOf("ticks");
        await sleep(50);
        expect(spanTree(trace)).toBe(
          ["SERVER DeskGateway:ticks", "  INTERNAL DeskGateway.ticks"].join(
            "\n",
          ),
        );
        expect(
          spans.finished.filter((span) => span.name === "DeskGateway:ticks"),
        ).toHaveLength(1);
      });

      it("nests an outgoing call under the provider that made it", async () => {
        const reply = client.next("looked-up");
        client.send("lookup");
        expect(await reply).toBe("looked-up");

        const trace = await traceOf("lookup");
        const host = downstreamUrl.replace("http://", "");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:lookup",
            "  INTERNAL DeskGateway.lookup",
            "    INTERNAL RatesService.lookup",
            `      INTERNAL http.GET ${host}`,
          ].join("\n"),
        );
      });

      it("correlates on the message's own trace id, a new one for every message", async () => {
        const first = client.next("you-are");
        client.send("whoami");
        const firstId = await first;
        const firstRoot = spanNamed(
          await traceOf("whoami"),
          "DeskGateway:whoami",
        );
        spans.reset();

        const second = client.next("you-are");
        client.send("whoami");
        const secondId = await second;
        const secondRoot = spanNamed(
          await traceOf("whoami"),
          "DeskGateway:whoami",
        );

        expect(firstId).toBe(firstRoot.spanContext().traceId);
        expect(secondId).toBe(secondRoot.spanContext().traceId);
        expect(firstId).not.toBe(secondId);
      });

      it("correlates concurrent messages on their own trace ids, read after yielding", async () => {
        // Log lines correlate on the id in the message's async store. A store
        // shared between messages would hand a slow handler the id of
        // whichever message wrote last.
        const delays = [40, 25, 10, 0];
        const replies = client.collect("you-were");
        for (const delayMs of delays) {
          client.send("whoami-later", delayMs);
        }

        const roots = await spans.waitFor((finished) => {
          const found = finished.filter(
            (span) => span.name === "DeskGateway:whoami-later",
          );
          return found.length === delays.length &&
            replies.length === delays.length
            ? found
            : undefined;
        });
        expect(new Set(replies).size).toBe(delays.length);
        expect(new Set(replies)).toEqual(
          new Set(roots.map((root) => root.spanContext().traceId)),
        );
      });

      it("keeps concurrent messages from two clients in traces of their own", async () => {
        // Sent together on two sockets, finishing in reverse order, each
        // calling a provider before and after it yields. Any span that lands
        // in another message's trace changes some trace's tree.
        const other = await connectClient(platform, port);
        try {
          const delays = [50, 40, 30, 20, 10, 0];
          const replies = [client.collect("visited"), other.collect("visited")];
          delays.forEach((delayMs, index) =>
            (index % 2 ? other : client).send("visit", {
              room: `room-${index}`,
              delayMs,
            }),
          );
          await spans.waitFor(() =>
            replies.flat().length === delays.length ? true : undefined,
          );
          expect(new Set(replies.flat())).toEqual(
            new Set(delays.map((_, index) => `seated:room-${index}`)),
          );

          await spans.waitFor((finished) => {
            const roots = finished.filter(
              (span) => span.name === "DeskGateway:visit",
            );
            return roots.length === delays.length ? roots : undefined;
          });
          const trees = treesByTrace(spans.finished);
          expect(trees.size).toBe(delays.length);
          for (const tree of trees.values()) {
            expect(tree).toBe(
              [
                "SERVER DeskGateway:visit",
                "  INTERNAL DeskGateway.visit",
                "    INTERNAL RoomsService.join",
                "    INTERNAL RoomsService.seat",
              ].join("\n"),
            );
          }
        } finally {
          other.close();
        }
      });

      it("records nothing for a message the ignore hook rejects, and the next one in full", async () => {
        const pong = client.next("pong");
        client.send("ping");
        expect(await pong).toBe("joined:ping");
        client.send("join", "after-ping");

        const trace = await traceOf("join");
        expect(spanTree(trace)).toBe(
          [
            "SERVER DeskGateway:join",
            "  INTERNAL DeskGateway.join",
            "    INTERNAL RoomsService.join",
          ].join("\n"),
        );
        // Not even the provider the ignored handler called.
        expect(spans.finished.map((span) => span.name)).toEqual(
          trace.map((span) => span.name),
        );
      });

      it("records nothing for a message the sampler declines", async () => {
        const reply = client.next("sampled-out-done");
        client.send("sampled-out");
        // The handler runs either way.
        expect(await reply).toBe("joined:nobody");

        client.send("join", "after-sample");
        const trace = await traceOf("join");
        expect(spans.finished.map((span) => span.name).sort()).toEqual(
          trace.map((span) => span.name).sort(),
        );
      });

      it.runIf(platform === "socket.io")(
        "still delivers the reply through an acknowledgement callback",
        async () => {
          expect(await client.ack!("count", "acked")).toBe(
            "joined:acked".length,
          );
          const trace = await traceOf("count");
          expect(spanTree(trace)).toBe(
            [
              "SERVER DeskGateway:count",
              "  INTERNAL DeskGateway.count",
              "    INTERNAL RoomsService.join",
            ].join("\n"),
          );
        },
      );
    });

    describe("with ws.ignore rejecting every message", () => {
      let app: INestApplication;
      let spans: OtelTestSpans;
      let client: TestClient;

      beforeAll(async () => {
        ({ app, spans, client } = await bootApp(platform, {
          ignore: () => true,
        }));
      });

      afterAll(async () => {
        client?.close();
        await app?.close();
      });

      it("switches gateway tracing off: handlers answer, fail and call out as before, and nothing is recorded", async () => {
        const reply = client.next("looked-up");
        client.send("lookup");
        expect(await reply).toBe("looked-up");

        const exception = client.next("exception");
        client.send("explode");
        await exception;

        const whoami = client.next("you-are");
        client.send("whoami");
        // Still a trace id of its own, for logs - just no spans.
        expect(await whoami).toEqual(expect.any(String));

        await sleep(100);
        expect(spans.finished).toEqual([]);
      });
    });
  },
);
