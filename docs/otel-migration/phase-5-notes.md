# OTel migration: Phase 5 notes

Phase 5 hardens OTel mode: span-tree integration suites for every protocol,
a run beside `instrumentation-http`, an end-to-end run through a real
Collector, and a load benchmark. The suites found five recorder and agent
bugs, which are fixed here. As before, the default (registry) mode is
unchanged.

## No collector credentials in OTel mode

```ts
const { ObserveModule } = createObserveModule({ opentelemetry: true });
ObserveModule.forRoot({}); // appKey, appSecret and serviceId now optional
```

- `forRoot` and `forRootAsync` take `OpenTelemetryObserveOptions` when
  `createObserveModule` was given `opentelemetry: true` or an object. Without
  it, the three fields stay required at compile time (`ObserveOptionsFor`).
- No worker thread and no flush timer start. Spans and metrics leave through
  the application's SDK, and the meter's runtime metrics still start.
- `forwardLogs` is now the only thing that ships to the Observe collector.
  It starts the worker only when `appKey`, `appSecret` and `serviceId` are
  all given. If any is missing, it is switched off with a warning.

## Span-tree suites

The suites use a shared harness, `src/testing/otel-harness.ts`. It gives
each suite its own provider and in-memory exporter, and `spanTree()`
draws a trace as indented `KIND name` lines. Most tests assert the whole
tree at once, so a span in the wrong place fails the test.

| Suite                               | Ports                                                 | Tests          |
| ----------------------------------- | ----------------------------------------------------- | -------------- |
| `protocols/http-fastify-otel`       | http-collection, http-fastify-collection, interceptor | 12             |
| `instrument/hostile-providers-otel` | hostile-providers (nestjs-cls proxies)                | 4              |
| `protocols/microservice-otel`       | microservice-tcp-collection, trace-propagation, Redis | 15 (1 skipped) |
| `protocols/grpc-otel`               | grpc-collection                                       | 10             |
| `protocols/jobs-otel`               | bull, bullmq scenarios, queue inheritance, schedule   | 22             |
| `protocols/graphql-otel`            | graphql-collection, plus concurrency and batching     | 14             |
| `protocols/ws-otel`                 | ws, socket.io, ws-scenarios                           | 34 (1 skipped) |
| `recorder/otel-coexistence`         | new: beside `instrumentation-http`                    | 3              |
| `recorder/otel-collector`           | new: through a real Collector                         | 2              |

The microservice suite skips one test because the installed
`@nestjs/microservices` 12.0.1 has no `setOnDispatchHook`. Propagation is
instead tested through a loopback transport shaped like upstream's
`ClientProxy`. Every test was mutation-checked: one or more mutations of
the behaviour each covers fail it (90+ mutations in all).

## Bugs found and fixed

| Bug                                                                                                                                    | Fix                                                                                                                               |
| -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Behind a pass-through interceptor (`@sentry/nestjs`'s shape) or nestjs-cls middleware, no handler or provider span was recorded at all | Nest runs the handler in the ended step's context. `currentSpan()` now walks up to the nearest span still open                    |
| GraphQL operations batched into one HTTP request nested under each other                                                               | `enterStep` parents on the operation's root and enters a cell of its own instead of swapping the span in the request's shared one |
| The microservice dispatch hook was itself instrumented (`Client.onDispatchHook`), and parented the server span                         | Hooks the agent installs are marked (`markUntraced`); the instance decorator leaves them alone                                    |
| gRPC spans were named after the bare method, merging same-named methods of different services                                          | `rpc.service` from the call's path; the span is named `package.Service/Method`                                                    |
| A second app in the same process sent its `@Cron`/`@Interval`/`@Timeout` runs to the first app's recorder                              | The explorer's original is parked on the prototype and re-wrapped on every boot, as the queue agents do                           |
| Under a foreign span, the module's own options object was instrumented (`Object.traceIdGenerator` spans)                               | Resolved options are skipped by the decorator                                                                                     |
| `OtelMetrics.register` was instrumented, showing as a span in every handler that created a metric                                      | `OtelMetrics` is skipped by the decorator                                                                                         |

## Beside instrumentation-http, and the Kubernetes Operator

`otel-coexistence.int-spec.ts` runs the real `@opentelemetry/instrumentation-http`:

- There is exactly one SERVER span, the SDK's, with the operation under it
  as INTERNAL.
- The SDK extracts the inbound `traceparent`, and `currentTraceId()`
  returns the trace it continued.
- With `outgoing.http` off, the SDK's CLIENT span lands under the handler
  that made the call.
- **New:** the route now reaches the SDK's SERVER span. The span becomes
  `GET /orders/:id` with `http.route`, instead of the bare `GET`. This
  uses the RPC metadata `instrumentation-http` leaves in context, the same
  slot its Express and Nest instrumentations fill. It is read through the
  registered context-key symbol, so `@opentelemetry/core` is not a
  dependency.

The recommended combination with the Operator's Node auto-instrumentation:

```yaml
# Instrumentation resource / pod env
OTEL_NODE_DISABLED_INSTRUMENTATIONS: nestjs-core,graphql
```

```ts
ObserveModule.forRoot({ outgoing: { http: false, database: false } });
```

The Operator keeps the wire: http, undici, pg, redis and the rest. Observe
provides the Nest structure: provider spans, protocol roots, GraphQL
resolvers and jobs. Each layer is recorded once.

## End-to-end through a real Collector

`npm run test:otel-collector` downloads the Collector core distribution
once into `node_modules/.cache/otelcol` (or uses `$OTELCOL_BIN`). It then
runs `otel-collector.int-spec.ts`:

- The app exports over OTLP/HTTP with `BatchSpanProcessor` and a periodic
  metric reader.
- The Collector writes what it received with its file exporter.
- The test asserts on that file: the span tree continuing an inbound
  `traceparent`, SERVER kind, `http.route` and status, `service.name`, the
  `@nestjs/observe` scope, a custom counter and the runtime metrics.

The suite is skipped in `npm run test:int` without a Collector binary.

## Benchmark

`scripts/bench/otel-overhead.mjs` (`npm run build` first) runs each variant
in a fresh child process. The endpoint is a controller over two providers,
one of them async. A parent process drives 32 keep-alive connections at it,
with 3s of warm-up and 10s measured, and runs the sink every exporter ships
to. Results are on Node 22 with 4 cores, two runs:

| Variant          | req/s       | p50 ms      | p99 ms      | CPU µs/req | spans/req |
| ---------------- | ----------- | ----------- | ----------- | ---------- | --------- |
| baseline         | 6568 / 7437 | 4.0 / 3.4   | 12.7 / 11.6 | 160 / 143  | –         |
| registry         | 3503 / 3779 | 7.8 / 6.9   | 19.8 / 19.1 | 304 / 287  | –         |
| otel             | 2865 / 3080 | 9.8 / 9.2   | 27.1 / 22.0 | 363 / 342  | 4         |
| nestjs-core      | 4867 / 4363 | 5.5 / 6.3   | 17.0 / 19.8 | 207 / 230  | 2         |
| http+otel        | 2219 / 2145 | 13.3 / 14.2 | 31.4 / 30.8 | 476 / 488  | 5         |
| http+nestjs-core | 3201 / 3382 | 8.9 / 8.5   | 25.9 / 22.2 | 317 / 303  | 3         |

CPU is the server process's, worker thread included.

- **OTel vs registry:** +55–60 µs per request (+20%), for the same tree
  plus SDK export. The registry shipped only ~10 bytes per request in the
  measured window. Most of its snapshots had not been shipped by then, so
  its figure is a lower bound.
- **Per span:** OTel mode costs about 50 µs per span over baseline;
  `instrumentation-nestjs-core` costs about 35. The difference is the
  instance decorator's proxy work for provider spans, which nestjs-core
  does not record. Observe records 4 spans to its 2: provider methods,
  not only the handler.
- **Beside `instrumentation-http`:** that costs about 110 µs per request
  on top of either. The Operator combination (`http+otel`) is about 330 µs
  over baseline. That is about 3.4× the request's own CPU on this trivial
  endpoint, and a smaller share on any endpoint that does real work.

The Phase 2 microbenchmark predicted this: OTel's context plus the cell
write costs more per step than the registry's `Map` copy. Moving to
`BatchSpanProcessor` closes the export part of the gap, not the per-step
part. Where to look next, if overhead matters:

- the proxy `get` trap's per-call allocation;
- `startSpan` attribute objects;
- `context.with` nesting per step.

## Semantic-convention gaps (not fixed)

All but the last are fixed since; see `semconv-gaps-notes.md`.

- Job roots have no `messaging.system`. The recorder does not know the
  driver; the queue agents could pass it.
- gRPC SERVER spans have no `rpc.grpc.status_code`. The end hook gets
  only the request.
- `graphql.operation.name` holds the root field (`createOrder`), not the
  document's operation name (`CreateOrder`). This is as Phase 2 decided.
- A bull callback handler that fails with `done(err)` gets
  `error.type: "failed"`, but its error is not recorded as an exception
  event.
- `ClientProxy` internals (`ClientTCP.connect`, `createSocket`, …) are
  instrumented as provider methods, which makes caller-side trees noisy.
  This is worth an exclusion like `ModulesContainer`'s.
- A GraphQL document with a syntax error still opens a step for its first
  field.
- The registry recorder still mis-parents batched GraphQL operations,
  because it shares `CALLER_METADATA_KEY`. Not fixed, since the registry
  is on its way out.

## Not done in this phase

- **OTel is not the default yet.** Flipping the default breaks every
  current user. They would need `@opentelemetry/api` and an SDK, and would
  lose the hosted dashboard. That belongs in a major release, together
  with the "drop" half of the plan's keep/drop list: the worker thread,
  shared buffer, wire contract, degraded ingest, credentials, profiling
  and source-context. This phase makes the OTel path self-sufficient, so
  that release is a deletion.
- **Still deferred:**
  - `captureRequest`;
  - `abandon` handling beyond the attribute;
  - injection for non-Nest producers;
  - RMQ/NATS reading only packet metadata;
  - the `node:http` client's parent;
  - summary quantiles;
  - OTel Logs API emission for `forwardLogs`;
  - `skipSpans`/`spanCollapse`, which are v2.

## Verification

- Unit: **788 passed**, 1 skipped (was 780). The new tests cover the OTel
  worker skip (3) and the OTel options types and `forwardLogs` rule (5).
- Integration (Redis, no Collector binary): **46 files passed, 4 skipped.
  390 passed, 62 skipped** (was 38 files, 278 passed).
- `npm run test:otel-collector`: 2 passed.
- Lint: 0 errors, 52 warnings (unchanged). Typecheck and build are clean.
