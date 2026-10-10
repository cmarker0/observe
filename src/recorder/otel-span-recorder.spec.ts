import { BadRequestException, Logger } from "@nestjs/common";
import * as api from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { RpcException } from "@nestjs/microservices";
import { AsyncLocalStorage } from "async_hooks";
import type { ObserveModuleOptionsWithDefaults } from "../interfaces/index.js";
import { JOB_TRACE_OPTION_KEY } from "../observe.constants.js";
import { JobTraceRunner } from "../protocols/job-trace-runner.js";
import { TraceSamplerService } from "../services/trace-sampler.service.js";
import { LogRedactor } from "../utils/log-redactor.js";
import { ObserveAttributes, OtelSpanRecorder } from "./otel-span-recorder.js";
import { OperationStart } from "./span-recorder.js";

/**
 * The behaviours `registry-span-recorder.spec.ts` pins for the registry
 * recorder, held against OpenTelemetry - plus what only spans can show: kinds,
 * attributes, status and parentage.
 */
describe("OtelSpanRecorder", () => {
  const TRACE_ID_KEY = "traceId";

  let als: AsyncLocalStorage<Map<string, unknown>>;
  let exporter: InMemorySpanExporter;
  let provider: BasicTracerProvider;
  let recorder: OtelSpanRecorder;
  let capture: boolean;

  const request = (
    correlationId: string,
    extra: Partial<OperationStart> = {},
  ): OperationStart => ({
    kind: "request",
    correlationId,
    protocol: "http",
    attributes: { method: "GET", originalUrl: "/orders?page=2" },
    sampling: ["http", { url: "/orders?page=2", method: "GET" }],
    ...extra,
  });

  const inStore = <T>(correlationId: string, fn: () => T): T =>
    als.run(new Map([[TRACE_ID_KEY, correlationId]]), fn);

  const step = (name: string) =>
    recorder.runStep({ className: "Svc", methodKey: name }, () => name);

  const flush = () => new Promise((resolve) => setTimeout(resolve, 5));

  const spans = () => exporter.getFinishedSpans();
  const span = (name: string) => {
    const found = spans().find((candidate) => candidate.name === name);
    if (!found) {
      throw new Error(
        `No span "${name}" in ${spans()
          .map((s) => s.name)
          .join(", ")}`,
      );
    }
    return found;
  };
  const parentOf = (child: ReadableSpan) =>
    child.parentSpanContext?.spanId ?? undefined;
  const idOf = (of: ReadableSpan) => of.spanContext().spanId;

  beforeAll(() => {
    api.context.setGlobalContextManager(
      new AsyncLocalStorageContextManager().enable(),
    );
  });

  afterAll(() => {
    api.context.disable();
  });

  beforeEach(() => {
    als = new AsyncLocalStorage();
    exporter = new InMemorySpanExporter();
    provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    recorder = new OtelSpanRecorder(als, api, { tracerProvider: provider });
    capture = true;
    recorder.attach(new LogRedactor(), {
      shouldCapture: () => capture,
    } as unknown as TraceSamplerService);
  });

  describe("parity with the registry recorder", () => {
    it("ends an operation as a SERVER span with its steps beneath it", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          operation!.setRoute("/orders");
          step("load");
          operation!.end(() => ({ statusCode: 200, userId: "u-1" }));
        }),
      );
      await flush();

      const root = span("GET /orders");
      expect(root.kind).toBe(api.SpanKind.SERVER);
      expect(root.attributes).toMatchObject({
        "http.request.method": "GET",
        "http.route": "/orders",
        "url.path": "/orders",
        "url.query": "page=2",
        "http.response.status_code": 200,
        "enduser.id": "u-1",
        [ObserveAttributes.CORRELATION_ID]: "req-1",
      });
      expect(root.status.code).toBe(api.SpanStatusCode.UNSET);

      const load = span("Svc.load");
      expect(load.kind).toBe(api.SpanKind.INTERNAL);
      expect(load.attributes["code.function.name"]).toBe("Svc.load");
      expect(parentOf(load)).toBe(idOf(root));
    });

    it("runs a job as a CONSUMER span in a trace of its own", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (outer) => {
          inStore("req-1", () =>
            recorder.runOperation(
              {
                kind: "job",
                correlationId: "req-1",
                job: { queueName: "emails", name: "welcome", id: "7" },
                messagingSystem: "bullmq",
              },
              (operation) => {
                step("send");
                operation!.end(() => ({ status: "completed" }));
              },
            ),
          );
          outer!.end();
        }),
      );
      await flush();

      const job = span("process emails");
      expect(job.kind).toBe(api.SpanKind.CONSUMER);
      expect(job.attributes).toMatchObject({
        "messaging.system": "bullmq",
        "messaging.destination.name": "emails",
        "messaging.message.id": "7",
        [ObserveAttributes.JOB_NAME]: "welcome",
      });
      expect(parentOf(job)).toBeUndefined();
      expect(job.spanContext().traceId).not.toBe(
        span("GET").spanContext().traceId,
      );
      expect(parentOf(span("Svc.send"))).toBe(idOf(job));
    });

    it("keeps an unrecorded operation's steps out of an open one", async () => {
      await inStore("shared", async () => {
        const operation = recorder.runOperation(request("shared"), (op) => {
          step("owned");
          inStore("shared", () =>
            recorder.runOperation(
              request("shared", { record: false }),
              (inner) => {
                expect(inner).toBeUndefined();
                step("stray");
              },
            ),
          );
          return op;
        });
        operation!.end();
        await flush();
      });

      expect(spans().map((s) => s.name)).toEqual(["Svc.owned", "GET"]);
    });

    it("hands a sampled-out operation no handle, and propagates the decision", () => {
      capture = false;

      const seen = inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => ({
          operation,
          span: recorder.activeSpan(),
          current: recorder.currentOperation(),
          traced: recorder.runStep(
            { className: "Svc", methodKey: "x" },
            (traced) => traced,
          ),
          context: api.trace.getSpan(api.context.active())?.spanContext(),
        })),
      );

      expect(seen).toMatchObject({
        operation: undefined,
        span: "untraced",
        current: undefined,
        traced: false,
      });
      // Valid, so a downstream service is told - and told "not sampled".
      expect(api.isSpanContextValid(seen.context!)).toBe(true);
      expect(seen.context!.traceFlags).toBe(api.TraceFlags.NONE);
      expect(spans()).toHaveLength(0);
    });

    it("ignores an abandon after the operation ended", async () => {
      const first = inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => operation!),
      );
      first.end();
      first.abandon();
      await flush();

      expect(span("GET").attributes).not.toHaveProperty(
        ObserveAttributes.ABANDONED,
      );
    });

    it("makes an entered step current until it is restored", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          const rootId = recorder.currentSpanId();
          const entered = recorder.enterStep({
            className: "Query",
            methodKey: "orders",
          })!;
          const inside = recorder.currentSpanId();
          step("resolve");
          entered.restore();
          const after = recorder.currentSpanId();
          entered.step.end();
          operation!.end();

          expect(inside).not.toBe(rootId);
          expect(after).toBe(rootId);
        }),
      );
      await flush();

      expect(parentOf(span("Svc.resolve"))).toBe(idOf(span("Query.orders")));
      expect(parentOf(span("Query.orders"))).toBe(idOf(span("GET")));
    });

    it("calls a step untraced outside any operation", () => {
      const traced = recorder.runStep(
        { className: "Svc", methodKey: "x" },
        (isTraced) => isTraced,
      );

      expect(traced).toBe(false);
      expect(spans()).toHaveLength(0);
    });

    it("ends an open step once, however often a driver reports it", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          const opened = recorder.openStep(
            { className: "pg", methodKey: "query" },
            { "db.system": "postgresql" },
          )!;
          opened.end();
          opened.end(new Error("late listener"));
          operation!.end();
        }),
      );
      await flush();

      const query = span("pg.query");
      expect(query.attributes["db.system"]).toBe("postgresql");
      expect(query.status.code).toBe(api.SpanStatusCode.UNSET);
      expect(query.events).toHaveLength(0);
    });
  });

  describe("errors", () => {
    it("records the error a job's driver reported on the run itself", async () => {
      inStore("job-1", () =>
        recorder.runOperation(
          {
            kind: "job",
            correlationId: "job-1",
            job: { queueName: "emails", name: "welcome", id: "7" },
          },
          (operation) => {
            step("send");
            // `done(err)`: the handler returned, so no step threw it.
            operation!.end(() => ({
              status: "failed",
              error: new TypeError("mailbox full password=hunter2"),
            }));
          },
        ),
      );
      await flush();

      const job = span("process emails");
      expect(job.status.code).toBe(api.SpanStatusCode.ERROR);
      expect(job.attributes["error.type"]).toBe("TypeError");
      const [event] = job.events;
      expect(event.name).toBe("exception");
      expect(event.attributes!["exception.type"]).toBe("TypeError");
      expect(JSON.stringify(event.attributes)).not.toContain("hunter2");
    });

    it("does not record a thrown error on the run a second time", async () => {
      const error = new Error("send failed");
      await inStore("job-1", () =>
        recorder.runOperation(
          { kind: "job", correlationId: "job-1", job: { queueName: "q" } },
          async (operation) => {
            await recorder
              .runStep({ className: "Svc", methodKey: "send" }, async () => {
                throw error;
              })
              .catch(() => undefined);
            operation!.end(() => ({ status: "failed", error }));
          },
        ),
      );
      await flush();

      expect(span("process q").events).toEqual([]);
      expect(span("Svc.send").events).toHaveLength(1);
      expect(span("process q").status.code).toBe(api.SpanStatusCode.ERROR);
    });

    describe("gRPC status code", () => {
      const grpcCall = async (thrown?: unknown) => {
        await inStore("grpc-1", () =>
          recorder.runOperation(
            {
              kind: "request",
              correlationId: "grpc-1",
              protocol: "GRPC",
              operationId: "FindOne",
              rpcService: "hero.HeroService",
            },
            async (operation) => {
              await recorder
                .runStep(
                  { className: "Hero", methodKey: "findOne" },
                  async () => {
                    if (thrown !== undefined) {
                      throw thrown;
                    }
                    return "ok";
                  },
                )
                .catch(() => undefined);
              operation!.end();
            },
          ),
        );
        await flush();
        return span("hero.HeroService/FindOne");
      };

      it("answers OK when nothing escaped the handler", async () => {
        const root = await grpcCall();
        expect(root.attributes["rpc.grpc.status_code"]).toBe(0);
        expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
      });

      it("takes the code an RpcException carries, leaving a client error unset", async () => {
        const root = await grpcCall(
          new RpcException({ code: 5, message: "no such hero" }),
        );
        expect(root.attributes["rpc.grpc.status_code"]).toBe(5);
        expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
      });

      it("fails the call on a code that is the server's failure", async () => {
        const root = await grpcCall(
          Object.assign(new Error("down"), { code: 14 }),
        );
        expect(root.attributes["rpc.grpc.status_code"]).toBe(14);
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
      });

      it("answers UNKNOWN for an error without a code, as grpc-js sends it", async () => {
        const root = await grpcCall(new Error("boom"));
        expect(root.attributes["rpc.grpc.status_code"]).toBe(2);
        expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
        expect(root.attributes["error.type"]).toBe("Error");
      });
    });

    it("records a thrown error redacted, and fails the operation it escaped", async () => {
      await inStore("req-1", () =>
        recorder.runOperation(
          request("req-1", { protocol: "TCP", attributes: undefined }),
          async (operation) => {
            await expect(
              recorder.runStep(
                { className: "Svc", methodKey: "boom" },
                async () => {
                  throw new Error("login failed password=hunter2");
                },
              ),
            ).rejects.toThrow();
            operation!.end();
          },
        ),
      );
      await flush();

      const boom = span("Svc.boom");
      expect(boom.status.code).toBe(api.SpanStatusCode.ERROR);
      const [event] = boom.events;
      expect(event.name).toBe("exception");
      expect(event.attributes!["exception.type"]).toBe("Error");
      expect(JSON.stringify(event.attributes)).not.toContain("hunter2");

      const root = spans().find((s) => s.kind === api.SpanKind.SERVER)!;
      expect(root.attributes["rpc.system"]).toBe("tcp");
      expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
      expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    });

    it("fails an HTTP operation whose GraphQL step threw, though it answered 200", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          const entered = recorder.enterStep({
            className: "Query",
            methodKey: "orders",
          })!;
          entered.restore();
          entered.step.end(new Error("resolver failed"));
          operation!.end(() => ({ statusCode: 200 }));
        }),
      );
      await flush();

      const root = span("GET");
      expect(root.attributes["http.response.status_code"]).toBe(200);
      expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
      expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    });

    it("leaves an HTTP operation unset when a handler raised a 4xx on purpose", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          expect(() =>
            recorder.runStep({ className: "Ctl", methodKey: "get" }, () => {
              throw new BadRequestException();
            }),
          ).toThrow();
          operation!.end(() => ({ statusCode: 200 }));
        }),
      );
      await flush();

      const root = span("GET");
      expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(400);
      expect(root.attributes[ObserveAttributes.ERROR_HANDLED]).toBe(true);
      expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    });

    it("does not fail the operation for an error a deeper step's caller caught", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          recorder.runStep({ className: "Ctl", methodKey: "get" }, () => {
            try {
              recorder.runStep({ className: "Svc", methodKey: "x" }, () => {
                throw new Error("caught");
              });
            } catch {
              // handled by the caller
            }
          });
          operation!.end(() => ({ statusCode: 200 }));
        }),
      );
      await flush();

      expect(span("Svc.x").status.code).toBe(api.SpanStatusCode.ERROR);
      expect(span("GET").status.code).toBe(api.SpanStatusCode.UNSET);
      expect(span("GET").attributes[ObserveAttributes.STATUS_CODE]).toBe(200);
    });

    it("captures a reported error on the current span with its tags", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          recorder.runStep({ className: "Svc", methodKey: "x" }, () =>
            recorder.captureError(new Error("bad"), { attempt: 2 }),
          );
          operation!.end();
        }),
      );
      await flush();

      const x = span("Svc.x");
      expect(x.attributes.attempt).toBe(2);
      expect(x.events[0].attributes!["exception.message"]).toBe("bad");
    });
  });

  describe("context", () => {
    it("nests the operation under a span another instrumentation made current", async () => {
      const foreign = provider.getTracer("http").startSpan("incoming");
      api.context.with(api.trace.setSpan(api.context.active(), foreign), () =>
        inStore("req-1", () =>
          recorder.runOperation(request("req-1"), (operation) =>
            operation!.end(),
          ),
        ),
      );
      foreign.end();
      await flush();

      expect(parentOf(span("GET"))).toBe(idOf(span("incoming")));
      // That span is the SERVER span already; this one does not repeat it.
      expect(span("GET").kind).toBe(api.SpanKind.INTERNAL);
    });

    it("never nests an operation under one of its own spans", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (outer) => {
          inStore("req-2", () =>
            recorder.runOperation(
              request("req-2", { protocol: "ws", operationId: "chat:send" }),
              (inner) => inner!.end(),
            ),
          );
          outer!.end();
        }),
      );
      await flush();

      expect(parentOf(span("chat:send"))).toBeUndefined();
    });

    it("hangs a hook-driven operation's steps under it once its callback returned", async () => {
      // The standalone GraphQL path: the store is entered, not run.
      await new Promise<void>((resolve) => {
        setImmediate(() => {
          als.enterWith(new Map([[TRACE_ID_KEY, "gql-1"]]));
          const operation = recorder.runOperation(
            {
              kind: "request",
              correlationId: "gql-1",
              protocol: "graphql",
              operationId: "Query.orders",
              operationName: "RecentOrders",
            },
            (handle) => handle,
          )!;
          const entered = recorder.enterStep({
            className: "OrdersResolver",
            methodKey: "orders",
          })!;
          setImmediate(() => {
            step("load");
            entered.restore();
            entered.step.end();
            operation.end();
            resolve();
          });
        });
      });
      await flush();

      const root = span("Query.orders");
      expect(root.attributes).toMatchObject({
        "graphql.operation.type": "query",
        // The document's name for the operation, not the root field.
        "graphql.operation.name": "RecentOrders",
      });
      expect(parentOf(span("OrdersResolver.orders"))).toBe(idOf(root));
      expect(parentOf(span("Svc.load"))).toBe(
        idOf(span("OrdersResolver.orders")),
      );
    });

    it("names a message transport's operation as a CONSUMER span", async () => {
      inStore("req-1", () =>
        recorder.runOperation(
          {
            kind: "request",
            correlationId: "req-1",
            protocol: "KAFKA",
            operationId: "orders.created",
          },
          (operation) => operation!.end(),
        ),
      );
      await flush();

      const root = span("process orders.created");
      expect(root.kind).toBe(api.SpanKind.CONSUMER);
      expect(root.attributes["messaging.system"]).toBe("kafka");
    });

    it("tags the current span through the sink a manual span hands out", async () => {
      await inStore("req-1", () =>
        recorder.runOperation(request("req-1"), async (operation) => {
          await recorder.runManualSpan("manual", async (sink) => {
            sink.setTags({ a: 1 });
            const active = recorder.activeSpan();
            expect(active).not.toBe("untraced");
            expect((active as { id: string }).id).toBe(sink.id);
            step("inside");
          });
          operation!.end();
        }),
      );
      await flush();

      expect(span("manual").attributes.a).toBe(1);
      expect(parentOf(span("Svc.inside"))).toBe(idOf(span("manual")));
    });
  });

  describe("propagation", () => {
    const TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
    const PARENT = "00f067aa0ba902b7";
    const traceparent = (flags = "01") => `00-${TRACE}-${PARENT}-${flags}`;

    beforeEach(() => {
      recorder = new OtelSpanRecorder(als, api, {
        tracerProvider: provider,
        propagator: new W3CTraceContextPropagator(),
        traceIdKey: TRACE_ID_KEY,
      });
      recorder.attach(new LogRedactor(), {
        shouldCapture: () => capture,
      } as unknown as TraceSamplerService);
    });

    /** What the recorder would send downstream from here. */
    const injected = () => {
      const carrier: Record<string, unknown> = {};
      recorder.injectContext(carrier);
      return carrier.traceparent as string | undefined;
    };

    it("continues the trace an inbound carrier names, and correlates logs on it", async () => {
      let correlated: unknown;
      inStore("req-1", () =>
        recorder.runOperation(
          request("req-1", { carrier: { traceparent: traceparent() } }),
          (operation) => {
            correlated = als.getStore()!.get(TRACE_ID_KEY);
            operation!.end();
          },
        ),
      );
      await flush();

      const root = span("GET");
      expect(root.kind).toBe(api.SpanKind.SERVER);
      expect(root.spanContext().traceId).toBe(TRACE);
      expect(parentOf(root)).toBe(PARENT);
      expect(correlated).toBe(TRACE);
      // The id the agent adopted is kept on the span.
      expect(root.attributes[ObserveAttributes.CORRELATION_ID]).toBe("req-1");
    });

    it("keeps the agent's correlation id for logs when asked to", () => {
      recorder = new OtelSpanRecorder(als, api, {
        tracerProvider: provider,
        propagator: new W3CTraceContextPropagator(),
        traceIdKey: TRACE_ID_KEY,
        logCorrelation: "correlation-id",
      });
      inStore("req-1", () =>
        recorder.runOperation(
          request("req-1", { carrier: { traceparent: traceparent() } }),
          (operation) => {
            expect(als.getStore()!.get(TRACE_ID_KEY)).toBe("req-1");
            operation!.end();
          },
        ),
      );
    });

    it("starts a trace of its own for a malformed carrier", async () => {
      inStore("req-1", () =>
        recorder.runOperation(
          request("req-1", { carrier: { traceparent: "00-nope" } }),
          (operation) => operation!.end(),
        ),
      );
      await flush();

      expect(parentOf(span("GET"))).toBeUndefined();
      expect(span("GET").spanContext().traceId).not.toBe(TRACE);
    });

    it("reads gRPC metadata and Kafka's Buffer header values", async () => {
      const grpc = { getMap: () => ({ traceparent: traceparent() }) };
      const kafka = { traceparent: Buffer.from(traceparent()) };
      for (const [protocol, carrier] of [
        ["GRPC", grpc],
        ["KAFKA", kafka],
      ] as const) {
        inStore("req-1", () =>
          recorder.runOperation(
            {
              kind: "request",
              correlationId: "req-1",
              protocol,
              operationId: protocol,
              carrier,
            },
            (operation) => operation!.end(),
          ),
        );
      }
      await flush();

      expect(parentOf(span("GRPC"))).toBe(PARENT);
      expect(parentOf(span("process KAFKA"))).toBe(PARENT);
    });

    it("keeps the caller's trace when sampled out, and passes the decision on", () => {
      capture = false;
      inStore("req-1", () =>
        recorder.runOperation(
          request("req-1", { carrier: { traceparent: traceparent() } }),
          (operation) => {
            expect(operation).toBeUndefined();
            expect(als.getStore()!.get(TRACE_ID_KEY)).toBe(TRACE);
            expect(injected()).toMatch(
              new RegExp(`^00-${TRACE}-[0-9a-f]{16}-00$`),
            );
          },
        ),
      );
    });

    it("leaves another instrumentation's span current for an operation it does not record", async () => {
      const foreign = provider.getTracer("http").startSpan("incoming");
      api.context.with(api.trace.setSpan(api.context.active(), foreign), () =>
        inStore("req-1", () =>
          recorder.runOperation(
            request("req-1", { record: false }),
            (operation) => {
              expect(operation).toBeUndefined();
              step("load");
              expect(injected()).toBe(
                `00-${foreign.spanContext().traceId}-${foreign.spanContext().spanId}-01`,
              );
            },
          ),
        ),
      );
      foreign.end();
      await flush();

      expect(spans().map((s) => s.name)).toEqual(["incoming"]);
    });

    it("sends an outgoing call's own span downstream", async () => {
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          const call = recorder.openStep({
            className: "http",
            methodKey: "GET api",
          })!;
          const carrier: Record<string, unknown> = {};
          recorder.injectContext(carrier, call);
          call.end();
          const [, traceId, spanId] = (carrier.traceparent as string).split(
            "-",
          );
          expect({ traceId, spanId }).toEqual({
            traceId: span("http.GET api").spanContext().traceId,
            spanId: idOf(span("http.GET api")),
          });
          operation!.end();
        }),
      );
      await flush();
    });

    it("links a job to the span that enqueued it, through the job's options", async () => {
      const runner = new JobTraceRunner(
        { traceIdKey: TRACE_ID_KEY } as ObserveModuleOptionsWithDefaults,
        recorder,
        als as AsyncLocalStorage<Map<never, unknown>>,
        new Logger("test"),
      );
      let stamped: Record<string, unknown> = {};
      const queue = {
        add(_name: string, _data: unknown, opts: Record<string, unknown>) {
          stamped = opts;
        },
      };
      runner.patchEnqueue(queue, () => 2);

      let enqueuer = "";
      inStore("req-1", () =>
        recorder.runOperation(request("req-1"), (operation) => {
          recorder.runStep({ className: "Svc", methodKey: "enqueue" }, () => {
            enqueuer = recorder.currentSpanId()!;
            queue.add("send", {}, {});
          });
          operation!.end();
        }),
      );

      let jobTraceId: unknown;
      runner.run(
        { queueName: "mail", name: "send", id: 1, opts: stamped, metadata: {} },
        () => {
          jobTraceId = als.getStore()!.get(TRACE_ID_KEY);
          step("deliver");
        },
      );
      await flush();

      const run = span("process mail");
      const request_ = span("GET");
      // A trace of its own, linked back.
      expect(parentOf(run)).toBeUndefined();
      expect(run.spanContext().traceId).not.toBe(
        request_.spanContext().traceId,
      );
      expect(run.links.map((link) => link.context.spanId)).toEqual([enqueuer]);
      // Logs correlate on each operation's own trace; the enqueuer's id is
      // what the run inherited as its correlation id.
      expect(stamped[JOB_TRACE_OPTION_KEY]).toBe(
        request_.spanContext().traceId,
      );
      expect(jobTraceId).toBe(run.spanContext().traceId);
      expect(run.attributes[ObserveAttributes.CORRELATION_ID]).toBe(
        request_.spanContext().traceId,
      );
    });
  });
});
