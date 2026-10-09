import { AsyncLocalStorage } from "async_hooks";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import { JobSnapshot, RequestSnapshot } from "../interfaces/index.js";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { TraceSamplerService } from "../services/trace-sampler.service.js";
import { RegistrySpanRecorder } from "./registry-span-recorder.js";
import { OperationStart } from "./span-recorder.js";

/**
 * The recorder over the real registry - the behaviour every agent now reaches
 * the registry through, and what a second `SpanRecorder` has to match.
 */
describe("RegistrySpanRecorder", () => {
  const TRACE_ID_KEY = "traceId";

  let als: AsyncLocalStorage<Map<string, unknown>>;
  let registry: OperationTraceRegistry;
  let recorder: RegistrySpanRecorder;
  let requests: RequestSnapshot[];
  let jobs: JobSnapshot[];
  let capture: boolean;

  const request = (
    correlationId: string,
    extra: Partial<OperationStart> = {},
  ): OperationStart => ({
    kind: "request",
    correlationId,
    protocol: "http",
    sampling: ["http", { url: "/", method: "GET" }],
    ...extra,
  });

  /** Runs `fn` in a fresh operation store carrying `correlationId`. */
  const inStore = <T>(correlationId: string, fn: () => T): T =>
    als.run(new Map([[TRACE_ID_KEY, correlationId]]), fn);

  const step = (name: string) =>
    recorder.runStep({ className: "Svc", methodKey: name }, () => name);

  /** Lets an operation's deferred end ship. */
  const flush = () => new Promise((resolve) => setTimeout(resolve, 5));

  beforeEach(() => {
    als = new AsyncLocalStorage();
    registry = new OperationTraceRegistry(als as never, false);
    recorder = new RegistrySpanRecorder(als, registry, TRACE_ID_KEY);
    requests = [];
    jobs = [];
    capture = true;
    recorder.attach(
      {
        insertRequestSnapshot: (snapshot: RequestSnapshot) =>
          requests.push(snapshot),
        insertJobSnapshot: (snapshot: JobSnapshot) => jobs.push(snapshot),
      } as unknown as ObserveAgentSharedBuffer,
      { shouldCapture: () => capture } as unknown as TraceSamplerService,
    );
  });

  it("ships an operation with its steps and the request it chose to capture", async () => {
    inStore("req-1", () =>
      recorder.runOperation(request("req-1"), (operation) => {
        step("load");
        operation!.end(() => ({
          statusCode: 200,
          captureRequest: () => ({ headers: { a: "b" } }),
        }));
      }),
    );
    await flush();

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      traceId: "req-1",
      protocol: "http",
      attributes: { statusCode: 200 },
      request: { headers: { a: "b" } },
    });
    expect(requests[0].traces).toHaveLength(1);
  });

  it("ships a job run as a job, under a key of its own", async () => {
    inStore("req-1", () =>
      recorder.runOperation(
        {
          kind: "job",
          correlationId: "req-1",
          job: { queueName: "emails", name: "welcome" },
        },
        (operation) => {
          step("send");
          // The inherited id stays free for the request that owns it.
          expect(registry.hasTrace("req-1")).toBe(false);
          operation!.end(() => ({ status: "completed" }));
        },
      ),
    );
    await flush();

    expect(requests).toHaveLength(0);
    expect(jobs).toEqual([
      expect.objectContaining({
        traceId: "req-1",
        queueName: "emails",
        status: "completed",
      }),
    ]);
  });

  it("keeps an unrecorded operation's steps out of an open one sharing its id", async () => {
    await inStore("shared", async () => {
      const operation = recorder.runOperation(request("shared"), (op) => op);
      step("owned");

      // A second operation adopting the same id - a retried `x-request-id`
      // that `http.ignore` drops, or a job inheriting it.
      inStore("shared", () =>
        recorder.runOperation(request("shared", { record: false }), (op) => {
          expect(op).toBeUndefined();
          step("stray");
        }),
      );

      operation!.end();
      await flush();
    });

    expect(requests).toHaveLength(1);
    expect(requests[0].traces.map((node) => node.methodKey)).toEqual(["owned"]);
  });

  it("hands a sampled-out operation no handle and nothing to record into", () => {
    capture = false;

    const seen = inStore("req-1", () =>
      recorder.runOperation(request("req-1"), (operation) => ({
        operation,
        span: recorder.activeSpan(),
        current: recorder.currentOperation(),
      })),
    );

    expect(seen).toEqual({
      operation: undefined,
      span: "untraced",
      current: undefined,
    });
  });

  it("does not let a late abandon drop a later operation that reused the id", async () => {
    const first = inStore("req-1", () =>
      recorder.runOperation(request("req-1"), (operation) => {
        step("a");
        return operation!;
      }),
    );
    first.end();
    await flush();

    inStore("req-1", () =>
      recorder.runOperation(request("req-1"), () => step("b")),
    );
    first.abandon();

    expect(registry.hasTrace("req-1")).toBe(true);
  });

  it("makes an entered step current until it is restored", () => {
    inStore("req-1", () =>
      recorder.runOperation(request("req-1"), () => {
        const entered = recorder.enterStep({
          className: "Query",
          methodKey: "orders",
        })!;
        const inside = recorder.currentSpanId();
        entered.restore();
        const after = recorder.currentSpanId();
        entered.step.end();

        expect(inside).toEqual(expect.any(String));
        expect(after).toBeUndefined();
      }),
    );
  });

  it("calls a step untraced outside any operation", () => {
    const traced = recorder.runStep(
      { className: "Svc", methodKey: "x" },
      (isTraced) => isTraced,
    );

    expect(traced).toBe(false);
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

    expect(requests[0].traces).toEqual([
      expect.objectContaining({
        methodKey: "query",
        tags: { "db.system": "postgresql" },
      }),
    ]);
    expect(requests[0].traces[0]).not.toHaveProperty("error");
  });
});
