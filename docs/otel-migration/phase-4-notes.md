# OTel migration: Phase 4 notes

Phase 4 moves metrics onto the OpenTelemetry Meter API in OTel mode and
finishes log correlation. As before, the default (registry) mode is
unchanged.

```ts
createObserveModule({
  opentelemetry: { tracerProvider, meterProvider }, // both optional
});
```

`OtelMetrics` (`src/metrics/otel-metrics.ts`) is provided only in OTel mode.
`TracerService`, `ObserveAgentWorker` and `NodeRuntimeMetricsService` take it
as optional. When it is there, they report through it and nothing goes to
the collector.

## Custom metrics

| `TracerService`   | OTel instrument                                                                    |
| ----------------- | ---------------------------------------------------------------------------------- |
| `counter()`       | asynchronous counter, read off the counter's cumulative per-series values          |
| `gauge()`         | asynchronous gauge, read off the gauge's current values                            |
| `summary()`       | histogram, recorded per observation; the summary's label becomes attribute `label` |
| labels and `tags` | attributes                                                                         |

Asynchronous instruments keep the existing classes as the source of truth.
Their series cap, finite-value guard and label validation all still apply,
and nothing changes for a caller. In-process quantiles (`p50`/`p95`/`p99`)
are not exported. The histogram's buckets come from the SDK's view
configuration, which is the OTel way to choose them.

Metric names are passed through as given. A name OTel rejects (it must
start with a letter) gets the SDK's own warning and records nothing.

## Runtime metrics: decided

The plan offered two options: map the runtime metrics, or recommend
`@opentelemetry/instrumentation-runtime-node`. This phase does the first,
using **the same names, units and attributes runtime-node reports**:

- Dashboards work with either source.
- `runtimeMetrics` keeps working with no extra dependency.
- The docs say to run one or the other.

| Metric                                                     | Instrument                | Unit | Attributes               |
| ---------------------------------------------------------- | ------------------------- | ---- | ------------------------ |
| `nodejs.eventloop.delay.{min,max,mean,stddev,p50,p90,p99}` | async gauge               | s    |                          |
| `nodejs.eventloop.utilization`                             | async gauge               | 1    |                          |
| `nodejs.eventloop.time`                                    | async counter             | s    | `nodejs.eventloop.state` |
| `v8js.gc.duration`                                         | histogram (0.01/0.1/1/10) | s    | `v8js.gc.type`           |
| `v8js.memory.heap.limit` / `.used`                         | async up-down counter     | By   | `v8js.heap.space.name`   |
| `v8js.heap.space.available_size` / `.physical_size`        | async up-down counter     | By   | `v8js.heap.space.name`   |
| `process.cpu.time`                                         | async counter             | s    | `cpu.mode`               |
| `process.memory.usage` (RSS)                               | async up-down counter     | By   |                          |

`process.*` comes from the process semconv. Runtime-node does not report it,
but host-metrics does. Observe already reported CPU and RSS, so they stay.

How this differs from the collector path:

- Readings are taken when the application's reader collects, so
  `runtimeMetricsInterval` does not apply.
- Event-loop delay and utilisation cover the time since the previous
  collection. A second reader on the same provider would split those windows.
- `NodeRuntimeMetricsService` does not start its own monitors in OTel mode.

## Logs

Phase 4 keeps to "trace correlation only".

- `traceIdKey` already holds the OTel trace id since Phase 3 (see
  `logCorrelation`).
- JSON log lines now also carry `spanId`, the span the line was written
  in, so a backend can place the line in the waterfall. This is OTel mode
  only; the registry's span ids never leave the snapshot.
- Text lines are unchanged (`Trace ID:` suffix).
- `forwardLogs` still ships to the collector. Emitting through the OTel Logs
  API is the plan's "later". It needs a logs SDK in the app, and the stdout
  forwarder would become a log-record emitter.

## Still tied to the collector in OTel mode

- `appKey`/`appSecret` are still required by the options type, and the worker
  still starts. With spans and metrics on OTel, it only carries forwarded
  logs. Making the credentials optional when `opentelemetry` is on (and not
  starting the worker unless `forwardLogs` is set) belongs with Phase 5's
  "OTel by default". Done in Phase 5; see `phase-5-notes.md`.

## Verification

- Unit: 773 → **780 passed**, 1 skipped (+7 in `otel-metrics.spec.ts`):
  - counter series with labels and tags
  - gauge readings
  - summary → histogram count and sum per label
  - registering the same metric twice
  - runtime names, units and attributes
  - GC duration by type, in seconds
  - stopping
- Integration (Redis only): **38 files passed, 3 skipped. 278 passed, 58
  skipped** (+3 in `otel-metrics.int-spec.ts`). The test boots a real app in
  OTel mode and checks:

  - a `TracerService` counter reaches the meter, and not the buffer
  - runtime metrics reach the meter, and not the buffer
  - a JSON log line carries the trace id and span id of the handler span

  Removing each of the three wiring points fails its test.

- Lint: 0 errors, 52 warnings (unchanged). Typecheck and build are clean.
