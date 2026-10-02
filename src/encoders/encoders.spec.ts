import { validateTelemetryPayload } from "../agent/telemetry-wire-contract.js";
import { Counter } from "../custom-metrics/counter.js";
import { Gauge } from "../custom-metrics/gauge.js";
import { Summary } from "../custom-metrics/summary.js";
import type { CustomMetric } from "../interfaces/index.js";
import { CustomMetricsEncoder } from "./custom-metrics.encoder.js";
import { JobSnapshotEncoder } from "./job-snapshot.encoder.js";
import { remapKeys } from "./remap-keys.util.js";
import { RequestSnapshotEncoder } from "./request-snapshot.encoder.js";
import { RuntimeMetricsEncoder } from "./runtime-metrics.encoder.js";

/**
 * The encoders define the wire format, and the ingestion DTOs decode it. Nothing
 * connects the two but agreement on single-letter keys, so these assert the
 * shape the agent actually emits - and, at the end, that the API accepts it.
 */
describe("remapKeys", () => {
  it("copies only the keys the map names", () => {
    expect(
      remapKeys({ name: "orders", extra: 1 }, { name: "n" }),
    ).toEqual({ n: "orders" });
  });

  it("ignores a source key that only the map's prototype answers to", () => {
    // `keyMap["toString"]` is a function, and truthy; without an own-key
    // check it would become the output key, spelt from its source code.
    const source = JSON.parse('{"toString": "x", "name": "orders"}');

    expect(remapKeys(source, { name: "n" })).toEqual({ n: "orders" });
  });
});

describe("CustomMetricsEncoder", () => {
  it("shortens the modelled keys", () => {
    const counter = new Counter("orders", "Orders placed");
    counter.increment(3);

    const encoded = CustomMetricsEncoder.encode(counter) as Record<
      string,
      unknown
    >;

    expect(encoded.n).toBe("orders");
    expect(encoded.t).toBe("counter");
    expect(encoded.d).toBe("Orders placed");
    expect(encoded.v).toEqual({ default: 3 });
  });

  it("carries a counter's increase", () => {
    const counter = new Counter("orders");
    counter.increment(5);
    counter.markFlushed();
    counter.increment(2);

    const encoded = CustomMetricsEncoder.encode(counter) as Record<
      string,
      unknown
    >;

    // `increase` is a prototype getter, which `for...in` cannot see - it has to
    // be copied across explicitly, and without it the backend has only
    // cumulative values to sum, which double counts.
    expect(encoded.iv).toEqual({ default: 2 });
  });

  it("carries a gauge's kind", () => {
    const gauge = new Gauge("cpu", { kind: "peak" });
    gauge.setValue(80);

    const encoded = CustomMetricsEncoder.encode(gauge) as Record<
      string,
      unknown
    >;

    // The kind decides how several reporters are merged; losing it turns a peak
    // into a sum.
    expect(encoded.k).toBe("peak");
    expect(encoded.v).toEqual({ default: 80 });
  });

  it("carries a summary's distribution", () => {
    const summary = new Summary("latency");
    for (const value of [10, 20, 30, 40, 50]) {
      summary.observe(value);
    }

    const encoded = CustomMetricsEncoder.encode(summary) as Record<
      string,
      unknown
    >;

    expect(encoded.q50).toBeDefined();
    expect(encoded.q95).toBeDefined();
    expect(encoded.q99).toBeDefined();
    expect(encoded.ct).toEqual({ default: 5 });
    expect(encoded.sm).toEqual({ default: 150 });
    expect(encoded.mx).toEqual({ default: 50 });
  });

  it("omits a summary's distribution when nothing was observed", () => {
    const summary = new Summary("latency");
    summary.observe(10);
    summary.markFlushed();

    const encoded = CustomMetricsEncoder.encode(summary) as Record<
      string,
      unknown
    >;

    // An empty window is not worth a row; reporting one would claim a
    // distribution that does not exist.
    expect(encoded.q50).toBeUndefined();
    expect(encoded.ct).toBeUndefined();
  });

  it("carries tags and the update timestamp off a class instance", () => {
    const counter = new Counter("orders");
    counter.tags = { region: "eu" };
    counter.increment();

    const encoded = CustomMetricsEncoder.encode(counter) as Record<
      string,
      unknown
    >;

    // Both are prototype getters on the metric classes, invisible to for...in
    // like `increase` - without the explicit copy the tags column stayed NULL
    // and every reading was stamped with server arrival time instead.
    expect(encoded.tg).toEqual({ region: "eu" });
    expect(encoded.lu).toBe(counter.lastUpdated);
  });

  it("does not emit a counter's increase for a gauge", () => {
    const gauge = new Gauge("level", {});
    gauge.setValue(5);

    const encoded = CustomMetricsEncoder.encode(gauge) as Record<
      string,
      unknown
    >;

    expect(encoded.iv).toBeUndefined();
  });

  it("produces a payload the ingestion contract accepts", () => {
    // One of each type, every reading filled in: a labelled counter with an
    // increase, a gauge with a kind, and a summary's whole distribution.
    const counter = new Counter("orders", "Orders placed", ["region"]);
    counter.increment({ region: "eu" }, 3);
    counter.markFlushed();
    counter.increment({ region: "eu" }, 2);
    const gauge = new Gauge("cpu", { kind: "peak" });
    gauge.setValue(80);
    const summary = new Summary("latency");
    for (const value of [10, 20, 30]) {
      summary.observe(value);
    }

    const errors = validateTelemetryPayload(
      {
        serviceId: "svc",
        snapshots: [],
        // Cast as `upsertCustomMetric` does: the encoder is typed for an
        // unlabelled metric.
        custom: [counter, gauge, summary].map((metric) =>
          CustomMetricsEncoder.encode(metric as CustomMetric),
        ),
      },
      { forbidUnknown: true },
    );

    expect(errors).toEqual([]);
  });
});

describe("RequestSnapshotEncoder", () => {
  const snapshot = {
    calledAt: "2026-01-01T00:00:00.000Z",
    traceId: "trace-1",
    startTimestamp: 1000,
    duration: 42.5,
    protocol: "http",
    operationId: "GET /orders",
    userId: "u-1",
    tags: { tenant: "acme" },
    attributes: { method: "GET", statusCode: 200, originalUrl: "/orders" },
    traces: [
      {
        name: "db.query",
        origin: "auto",
        className: "OrdersRepository",
        methodKey: "find",
        duration: 12,
        startOffset: 3,
        spanId: "span-1",
        tags: { table: "orders" },
        children: [
          {
            name: "cache.get",
            origin: "auto",
            className: "CacheService",
            methodKey: "get",
            duration: 1,
            children: [],
          },
        ],
      },
    ],
  } as never;

  it("shortens the snapshot keys", () => {
    const encoded = RequestSnapshotEncoder.encode(snapshot) as Record<
      string,
      any
    >;

    expect(encoded.ti).toBe("trace-1");
    expect(encoded.op).toBe("GET /orders");
    expect(encoded.p).toBe("http");
    expect(encoded.d).toBe(42.5);
    expect(encoded.u).toBe("u-1");
    expect(encoded.tg).toEqual({ tenant: "acme" });
  });

  it("shortens the attribute keys", () => {
    const encoded = RequestSnapshotEncoder.encode(snapshot) as Record<
      string,
      any
    >;

    expect(encoded.a).toEqual({
      m: "GET",
      sc: 200,
      ou: "/orders",
    });
  });

  it("encodes the trace tree recursively", () => {
    const encoded = RequestSnapshotEncoder.encode(snapshot) as Record<
      string,
      any
    >;
    const [span] = encoded.t;

    expect(span.n).toBe("db.query");
    expect(span.c).toBe("OrdersRepository");
    expect(span.m).toBe("find");
    expect(span.d).toBe(12);
    expect(span.so).toBe(3);
    expect(span.s).toBe("span-1");

    // Children carry the same shortened keys at every depth; a tree encoded only
    // at the top level would arrive with unreadable nested spans.
    expect(span.ch[0].n).toBe("cache.get");
    expect(span.ch[0].c).toBe("CacheService");
  });

  it("produces a payload the ingestion contract accepts", () => {
    const encoded = RequestSnapshotEncoder.encode(snapshot);

    const errors = validateTelemetryPayload({
      serviceId: "svc",
      snapshots: [encoded],
    });

    // The encoder and the API agree on nothing but these key names. If they
    // drift, the agent ships batches the API silently rejects.
    expect(errors).toEqual([]);
  });

  it("relies on the registry having stripped startTimestamp", () => {
    // `startTimestamp` is mapped (to `st`), so the encoder will happily ship it.
    // What keeps it off the wire is `OperationTraceRegistry.endTrace` deleting
    // it first - and that is load-bearing rather than tidiness: the API's
    // ValidationPipe runs with `forbidNonWhitelisted`, and `st` is not a field
    // it declares, so a snapshot that still carried it would be rejected
    // outright with a 400.
    const stillHasIt = RequestSnapshotEncoder.encode(snapshot) as Record<
      string,
      unknown
    >;
    expect(stillHasIt).toHaveProperty("st");

    const errors = validateTelemetryPayload(
      { serviceId: "svc", snapshots: [stillHasIt] },
      { forbidUnknown: true },
    );
    expect(errors).toContain("snapshots[0].st: not declared by the contract");

    // As the registry actually hands it over, there is no `st` and the payload
    // validates - which the test above this one already asserts.
    const asRegistryEmitsIt = { ...(snapshot as Record<string, unknown>) };
    delete asRegistryEmitsIt.startTimestamp;
    expect(
      RequestSnapshotEncoder.encode(asRegistryEmitsIt as never),
    ).not.toHaveProperty("st");
  });

  it("omits the children key on a leaf span", () => {
    const leafOnly = {
      ...(snapshot as Record<string, any>),
      traces: [
        {
          origin: "auto",
          className: "Svc",
          methodKey: "run",
          duration: 1,
          spanId: "s1",
        },
      ],
    } as never;

    const encoded = RequestSnapshotEncoder.encode(leafOnly) as Record<
      string,
      any
    >;

    // A `ch: []` on every leaf would roughly double the payload of a deep trace
    // for no information.
    expect(encoded.t[0]).not.toHaveProperty("ch");
  });

  it("ships a collapsed node with the keys the contract already declares", () => {
    // As the registry hands it over: `startTimestamp` already stripped.
    const asRegistryEmitsIt = { ...(snapshot as Record<string, any>) };
    delete asRegistryEmitsIt.startTimestamp;
    const collapsed = {
      ...asRegistryEmitsIt,
      traces: [
        {
          origin: "auto",
          className: "ValidationPipe",
          methodKey: "transform",
          name: "ValidationPipe.transform ×27",
          tags: { "observe.collapsed": 27 },
          duration: 40,
          startOffset: 2,
          spanId: "s1",
        },
      ],
    } as never;

    const encoded = RequestSnapshotEncoder.encode(collapsed) as Record<
      string,
      any
    >;

    // The count rides in `t`, so the node introduces no key of its own and an
    // older collector accepts it unchanged.
    expect(encoded.t[0].t).toEqual({ "observe.collapsed": 27 });
    expect(Object.keys(encoded.t[0]).sort()).toEqual(
      ["c", "d", "m", "n", "o", "s", "so", "t"].sort(),
    );
    expect(
      validateTelemetryPayload(
        { serviceId: "svc", snapshots: [encoded] },
        { forbidUnknown: true },
      ),
    ).toEqual([]);
  });
});

describe("JobSnapshotEncoder", () => {
  const jobSnapshot = {
    id: "job-1",
    traceId: "trace-job-1",
    name: "send-welcome",
    queueName: "emails",
    status: "completed",
    calledAt: "2026-01-01T00:00:00.000Z",
    startTimestamp: 1000,
    duration: 900,
    tags: { attempt: 1 },
    traces: [
      {
        origin: "auto",
        className: "MailService",
        methodKey: "send",
        duration: 800,
        spanId: "span-1",
        children: [
          {
            origin: "auto",
            className: "SmtpClient",
            methodKey: "connect",
            duration: 50,
            children: [],
          },
        ],
      },
    ],
  } as never;

  it("shortens the job keys", () => {
    const encoded = JobSnapshotEncoder.encode(jobSnapshot) as Record<
      string,
      any
    >;

    expect(encoded).toMatchObject({
      i: "job-1",
      ti: "trace-job-1",
      n: "send-welcome",
      q: "emails",
      s: "completed",
      c: "2026-01-01T00:00:00.000Z",
      d: 900,
      tg: { attempt: 1 },
    });
  });

  it("carries the queue metadata when the agent reports it", () => {
    const withQueueMetadata = {
      ...(jobSnapshot as Record<string, any>),
      enqueuedAt: "2026-01-01T00:00:00.000Z",
      waitDuration: 120,
      attemptsMade: 2,
      maxAttempts: 5,
    } as never;

    const encoded = JobSnapshotEncoder.encode(withQueueMetadata) as Record<
      string,
      any
    >;

    expect(encoded).toMatchObject({
      ea: "2026-01-01T00:00:00.000Z",
      wd: 120,
      am: 2,
      ma: 5,
    });
  });

  it("omits queue metadata an older agent does not report", () => {
    const encoded = JobSnapshotEncoder.encode(jobSnapshot) as Record<
      string,
      unknown
    >;

    // Those columns are nullable precisely so "not reported" stays distinct
    // from a genuine zero wait.
    for (const key of ["ea", "wd", "am", "ma"]) {
      expect(encoded).not.toHaveProperty(key);
    }
  });

  it("relies on the registry having stripped startTimestamp", () => {
    // Same coupling as the request encoder: `startTimestamp` is mapped, and it
    // is the registry that removes it before the snapshot is handed over.
    const asRegistryEmitsIt = { ...(jobSnapshot as Record<string, unknown>) };
    delete asRegistryEmitsIt.startTimestamp;

    expect(
      JobSnapshotEncoder.encode(asRegistryEmitsIt as never),
    ).not.toHaveProperty("st");
  });

  it("encodes spans with the same map the request encoder uses", () => {
    const encoded = JobSnapshotEncoder.encode(jobSnapshot) as Record<
      string,
      any
    >;

    // Both share TRACE_KEY_MAP, so a span reads identically whichever kind of
    // operation produced it - one shape for the reader, not two.
    expect(encoded.t[0]).toMatchObject({
      s: "span-1",
      c: "MailService",
      m: "send",
      d: 800,
    });
    expect(encoded.t[0].ch[0]).toMatchObject({ c: "SmtpClient" });
  });

  it("carries a failure with its reason", () => {
    const failed = {
      ...(jobSnapshot as Record<string, any>),
      status: "failed",
      error: { message: "SMTP timeout" },
    } as never;

    const encoded = JobSnapshotEncoder.encode(failed) as Record<string, any>;

    expect(encoded.s).toBe("failed");
    expect(encoded.e).toMatchObject({ message: "SMTP timeout" });
  });

  it("produces a payload the ingestion contract accepts", () => {
    const encoded = JobSnapshotEncoder.encode(jobSnapshot);

    const errors = validateTelemetryPayload({
      serviceId: "svc",
      snapshots: [],
      jobs: [encoded],
    });

    expect(errors).toEqual([]);
  });
});

describe("RuntimeMetricsEncoder", () => {
  const metrics = (overrides: Record<string, unknown> = {}) =>
    ({
      cpu: { user: 1, system: 2, percentageUsed: 33 },
      memory: {
        rss: 100,
        heapTotal: 200,
        heapUsed: 150,
        external: 10,
        arrayBuffers: 5,
        percentageUsed: 44,
      },
      gc: { count: 3, totalDuration: 12, breakdown: null },
      eventLoop: { lag: 7, utilization: 0.5 },
      ...overrides,
    }) as never;

  it("shortens every section", () => {
    const encoded = RuntimeMetricsEncoder.encode(metrics()) as Record<
      string,
      any
    >;

    expect(encoded.c).toEqual({ u: 1, s: 2, p: 33 });
    expect(encoded.m).toEqual({
      r: 100,
      ht: 200,
      hu: 150,
      e: 10,
      ab: 5,
      p: 44,
    });
    expect(encoded.e).toEqual({ l: 7, u: 0.5 });
  });

  it("shortens the GC section and its breakdown", () => {
    const encoded = RuntimeMetricsEncoder.encode(
      metrics({
        gc: {
          count: 4,
          totalDuration: 20,
          breakdown: {
            minor: { count: 3, duration: 12 },
            major: { count: 1, duration: 8 },
            incremental: { count: 0, duration: 0 },
          },
        },
      }),
    ) as Record<string, any>;

    expect(encoded.g).toMatchObject({ c: 4, td: 20 });
    expect(encoded.g.b).toEqual({
      m: { count: 3, duration: 12 },
      j: { count: 1, duration: 8 },
      i: { count: 0, duration: 0 },
    });
  });

  it("omits the breakdown for a window with no categorised collection", () => {
    const encoded = RuntimeMetricsEncoder.encode(metrics()) as Record<
      string,
      any
    >;

    // Carried through as null rather than omitted. The recorder reads it as
    // `runtime.g.b?.m?.count ?? 0`, so null and absent behave identically -
    // both mean "no categorised collection in this window".
    expect(encoded.g).toMatchObject({ c: 3, td: 12 });
    expect(encoded.g.b).toBeNull();
  });

  it("preserves zero readings rather than dropping them", () => {
    const encoded = RuntimeMetricsEncoder.encode(
      metrics({
        cpu: { user: 0, system: 0, percentageUsed: 0 },
        eventLoop: { lag: 0, utilization: 0 },
      }),
    ) as Record<string, any>;

    // An idle process is a real measurement; a truthiness check here would make
    // "completely idle" indistinguishable from "not reported".
    expect(encoded.c).toEqual({ u: 0, s: 0, p: 0 });
    expect(encoded.e).toEqual({ l: 0, u: 0 });
  });

  it("matches the paths the recorder destructures", () => {
    const encoded = RuntimeMetricsEncoder.encode(metrics());

    // TelemetryRecorderService reads exactly these when building an
    // app_profiler_snapshot row; a renamed key becomes a silently null column
    // rather than an error.
    expect(encoded).toMatchObject({
      c: {
        u: expect.any(Number),
        s: expect.any(Number),
        p: expect.any(Number),
      },
      m: {
        r: expect.any(Number),
        ht: expect.any(Number),
        hu: expect.any(Number),
        e: expect.any(Number),
        p: expect.any(Number),
      },
      g: { c: expect.any(Number), td: expect.any(Number) },
      e: { l: expect.any(Number), u: expect.any(Number) },
    });
  });

  it("produces a payload the ingestion contract accepts", () => {
    // With a breakdown in it: the default sample's is null, which the contract
    // reads as absent, and a contract that typed every kind as a number went
    // unnoticed behind it.
    const encoded = RuntimeMetricsEncoder.encode(
      metrics({
        gc: {
          count: 4,
          totalDuration: 20,
          breakdown: {
            minor: { count: 3, duration: 12 },
            major: { count: 1, duration: 8 },
            incremental: { count: 0, duration: 0 },
          },
        },
      }),
    );

    const errors = validateTelemetryPayload(
      { serviceId: "svc", snapshots: [], runtime: encoded },
      { forbidUnknown: true },
    );

    expect(errors).toEqual([]);
  });

  describe("deep traces", () => {
    /** A balanced tree `depth` levels below the root. */
    const trace = (depth: number): any => ({
      id: "a",
      name: "n",
      type: "http",
      startTime: 1,
      duration: 2,
      children: depth > 0 ? [trace(depth - 1), trace(depth - 1)] : [],
    });

    // Guards against re-nesting the child walk inside the key loop: that made
    // the encoder visit keys^depth nodes, and a ten-level trace - an ordinary
    // queued job - stopped the event loop for the lifetime of the process.
    it.each([
      [
        "a job snapshot",
        (t: any) => JobSnapshotEncoder.encode({ traces: [t] } as any),
      ],
      [
        "a request snapshot",
        (t: any) => RequestSnapshotEncoder.encode({ traces: [t] } as any),
      ],
    ])(
      "encodes a deeply nested trace in linear time for %s",
      (_label, encode) => {
        const startedAt = Date.now();
        const encoded = encode(trace(12)) as any;

        expect(Date.now() - startedAt).toBeLessThan(2_000);
        // Every level survived rather than being flattened away.
        let node = encoded.t[0];
        let levels = 0;
        while (node.ch?.length) {
          node = node.ch[0];
          levels += 1;
        }
        expect(levels).toBe(12);
      },
    );
  });
});

/**
 * The four "produces a payload the ingestion contract accepts" tests above are
 * only worth anything if the contract can actually fail. These are the negative
 * cases that keep it honest - a validator that silently returned `[]` would let
 * every one of them pass.
 */
describe("telemetry wire contract", () => {
  const valid = { serviceId: "svc", snapshots: [{ ti: "trace-1", d: 1 }] };

  it("accepts a well-formed batch", () => {
    expect(validateTelemetryPayload(valid)).toEqual([]);
  });

  it("reports a missing required field by path", () => {
    expect(validateTelemetryPayload({ snapshots: [] })).toEqual([
      "serviceId: required",
    ]);
  });

  it("reports a field of the wrong type", () => {
    expect(
      validateTelemetryPayload({ ...valid, serviceId: 1 as never }),
    ).toEqual(["serviceId: expected a string, got number"]);
  });

  it("descends into nested shapes", () => {
    expect(
      validateTelemetryPayload({
        serviceId: "svc",
        snapshots: [{ ti: "trace-1", a: { sc: "200" } }],
      }),
    ).toEqual(["snapshots[0].a.sc: expected a number, got string"]);
  });

  it("descends into spans at any depth", () => {
    const errors = validateTelemetryPayload(
      {
        serviceId: "svc",
        snapshots: [
          { ti: "trace-1", t: [{ n: "outer", ch: [{ n: "inner", zz: 1 }] }] },
        ],
      },
      { forbidUnknown: true },
    );

    expect(errors).toEqual([
      "snapshots[0].t[0].ch[0].zz: not declared by the contract",
    ]);
  });

  it("ignores undeclared keys unless asked to forbid them", () => {
    // Matches a plain `validate()` on the API side, which strips rather than
    // rejects - only the pipe configured with `forbidNonWhitelisted` refuses.
    const payload = {
      serviceId: "svc",
      snapshots: [{ ti: "trace-1", zz: 1 }],
    };

    expect(validateTelemetryPayload(payload)).toEqual([]);
    expect(validateTelemetryPayload(payload, { forbidUnknown: true })).toEqual([
      "snapshots[0].zz: not declared by the contract",
    ]);
  });

  it("rejects an array where an object belongs, and vice versa", () => {
    expect(
      validateTelemetryPayload({ serviceId: "svc", snapshots: {} as never }),
    ).toEqual(["snapshots: expected an array"]);
    expect(
      validateTelemetryPayload({
        serviceId: "svc",
        snapshots: [],
        runtime: [] as never,
      }),
    ).toEqual(["runtime: expected an object"]);
  });
});
