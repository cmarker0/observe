# OTel migration — Phase 0 findings

Spike output for the plan in `plan.md` (phases 0–5). Covers how the
registry and the agents fit together, the behavioural baseline, corrections to
the plan, and the `SpanRecorder` seam. Phase 1 has since implemented the
seam (`src/recorder/span-recorder.ts`); see `phase-1-notes.md` for where it
differs from the sketch this doc describes.

## Baseline

- Install with `npm ci --legacy-peer-deps`, as CI does. A plain `npm ci`
  fails with ERESOLVE (typeorm wants ioredis 5, the repo pins 6).
- The integration suites need Redis, Postgres (port 54321), MySQL and Mongo
  (port 27027). Each one skips itself when its server doesn't answer. CI
  provides all four as services. The local baseline below had Redis only, so
  the Postgres, MySQL and Mongo query-span suites skipped. Those suites cover
  the outgoing patches, which the plan drops anyway.
- Unit tests (`npm test`) pass: 739 passed, 1 skipped.
- Integration tests (`npm run test:int`) pass: see the end of this doc.

## How entangled the registry is

Agents use only a small surface of `OperationTraceRegistry`:

| Caller                                                    | Registry calls                                                                                                           |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| HTTP                                                      | `hasTrace`, `startTrace`, `addRouteMetadataToTrace`, `endTrace`, `pluckSnapshot`, `abandonTrace`, `getRedactor`          |
| RPC / gRPC, WS                                            | `startTrace`, `endTrace`, `pluckSnapshot`                                                                                |
| Jobs (Bull, BullMQ, queue, schedule via `JobTraceRunner`) | `startTrace`, `endTrace`, `pluckSnapshot`                                                                                |
| GraphQL                                                   | `startTrace`, `addGraphQLMetadataToTrace`, `internalStartTraceStep`, `internalEndTraceStep`, `endTrace`, `pluckSnapshot` |
| Instance decorator                                        | `internalStartTraceStep`, `internalEndTraceStep`                                                                         |
| Outgoing recorder                                         | `internalStartTraceStep`, `getActiveSpan`, `internalEndTraceStep`                                                        |
| `TracerService`                                           | `createManualSpan`, `getActiveSpan`, `hasTrace`, `captureError`                                                          |

**The risky parts are all internal.** Agents never see ref counting, the 1s
`SNAPSHOT_COMPLETION_TIMEOUT_MS` wait, parent links or tree assembly. With
OTel these pieces just disappear, because each span exports on its own and a
leaked child simply ends late. The re-entrancy flag lives in the decorator
and works with either backend. **Phase 1 should stay within its estimate.**

The coupling that does matter is all in the _glue around_ the registry:

1. **Every agent repeats the end sequence**: `setTimeout` → `endTrace` →
   `await pluckSnapshot` → `observeAgentSharedBuffer.insert*`. HTTP
   also post-processes the snapshot (`captureRequest`) in between. The seam
   folds all of this into `OperationHandle.end()`, with lazy
   `captureRequest`.
2. **Agents read reserved store keys directly.** They read
   `CALLER_METADATA_KEY` and `TRACE_REGISTRY_KEY ?? traceIdKey` from the ALS
   `Map`, and the code for it is copied into 8 files. The seam replaces this
   with `currentOperation()`, `currentIds()` and step context the recorder
   owns.
3. **HTTP duplicate-id handling** (`hasTrace` → mint a registry key) is
   registry bookkeeping and moves into the registry recorder.
4. **`TraceSpanDelegate` (public API) writes into a plain tags object** that
   the registry node holds, so the registry and the delegate share memory.
   It needs to write through a sink (`SpanTagSink`) instead. The public
   class can keep its shape.
5. **The outgoing recorder sets tags by grabbing the node** via
   `getActiveSpan` and mutating it. This becomes `openStep(name, tags)`.

## The hard case: GraphQL

The GraphQL driver hooks (`onRequestStart` / `onRequestEnd`) _return_
into execution instead of wrapping it. To make resolver spans nest under the
operation span, the agent:

- mutates `CALLER_METADATA_KEY` on the shared store and restores it at the
  end, and
- with no enclosing transport, installs a store with `enterWith`. The source
  already flags this as able to cross-attribute operations in a shared
  context.

OTel's context API has `context.with(ctx, fn)` and nothing equivalent to
`attach`. The seam isolates this as `enterStep()`, which only GraphQL may use.
For the OTel recorder, the options are:

- **(a) Own ALS slot.** The recorder keeps "current step" in its own
  `AsyncLocalStorage<Span>`. It resolves the parent as slot first, then
  `trace.getActiveSpan()`, and `runStep` sets both. This is simple, but it
  keeps one store write per step on the hot path.
- **(b) Wrapping hook.** Wrap `execute` in Apollo/Mercurius, the way
  `@opentelemetry/instrumentation-graphql` does. This gives real
  `context.with`, but it's driver-specific.
- **(c) Delegate.** Recommend `instrumentation-graphql` and keep only
  operation naming, parsing and redaction.

**Recommendation:** use (a) for v1 to keep parity, and evaluate (b) in
Phase 5. Note that (a) partly undoes the hot-path win the plan claims for
dropping the `Map` copy. Benchmark it before promising it.

## Corrections to the plan

- **Function-form `tracesSampleRate` is a head sampler, not a tail one.**
  `TraceSamplerService.shouldCapture` runs _before_ `startTrace` and only sees
  protocol attributes (url/method, transport/ctx, gRPC call, operationId,
  gateway/pattern). It maps directly to an OTel `Sampler` in v1, so don't
  defer it. Only `skipSpans` and `spanCollapse` need the finished tree and
  stay deferred to v2.
- **There is no root span today.** The "operation" is the snapshot, and the
  first instrumented handler call becomes a top-level span. The OTel recorder
  will add a real SERVER/CONSUMER span above it. That changes depth by one
  level, so the Phase 5 span-tree assertions must be written for the new
  shape rather than ported 1:1.
- **Root-error → status-code logic must move with it.** Several rules live in
  `endTrace`/`internalEndTraceStep`:

  - A top-level span that throws sets `errorStatusCode`.
  - `IntrinsicException` maps to 4xx, anything else to 500.
  - That error code overrides a transport's 2xx, which matters for GraphQL
    returning 200 and for RPC, which has no status at all.

  In OTel this becomes `setStatus(ERROR)` on the operation span, plus
  `error.type`, plus `http.response.status_code` when there is one. The
  handled/unhandled split has no direct semconv equivalent. Keep it as an
  attribute.

- **gRPC defers sampling and `ignore` by one `setTimeout` tick** (see
  `startGrpcRequestTracing`), because `call.operationId` isn't ready
  earlier. `runOperation` can't sample synchronously for gRPC, so the gRPC
  agent needs to open the operation inside that tick.
- **`endRpcRequestTracing` reads only `traceIdKey`**, never
  `TRACE_REGISTRY_KEY`. That's harmless today because RPC never mints a
  separate key, but it's the kind of drift the seam removes.
- **Log correlation already joins on span id.** The stdout forwarder reads
  `CALLER_METADATA_KEY` as the span id. With OTel that becomes
  `span.spanContext().spanId`, and `traceIdKey` becomes either the OTel trace
  id or the correlation id attribute. Pick one in Phase 3. Downstream log
  queries depend on it.

## Keep / drop list

The keep/drop list from the plan stands, with one change: function-form
`tracesSampleRate` moves from _defer_ to _keep, as a `Sampler`_.

## Proposed Phase 1 order

1. Make `TraceSpanDelegate` write through `SpanTagSink`. Public API unchanged.
2. Build `RegistrySpanRecorder` over the existing registry and buffer, and
   pass it the ALS. Add unit tests that run against the interface, so the OTel
   recorder can reuse them.
3. Migrate in this order, keeping `test:int` green after each step:
   - the instance decorator (`runStep`)
   - the outgoing recorder (`openStep`)
   - `TracerService`
   - WS
   - RPC/gRPC
   - `JobTraceRunner`
   - HTTP
   - GraphQL (`enterStep`) last
4. Remove the direct `OperationTraceRegistry` and `ObserveAgentSharedBuffer`
   injection from the agents. Enforce it with an oxlint
   `no-restricted-imports` rule.

## Integration baseline

With local Redis and no Postgres, MySQL or Mongo:
**35 files passed, 3 skipped. 268 tests passed, 58 skipped. Exit 0.**
The skipped files are presumably the query-span suites for the three
missing databases. This is the parity bar for Phase 1. For a full baseline,
run the same command in CI or with all four services up.
