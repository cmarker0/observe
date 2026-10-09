# OTel migration — Phase 1 notes

Phase 1 puts the existing registry behind a `SpanRecorder` seam. Every
caller now records spans through `SpanRecorder`
(`src/recorder/span-recorder.ts`), and `RegistrySpanRecorder` implements it
over the unchanged `OperationTraceRegistry` and shared buffer:

- the HTTP, RPC/gRPC, WS and GraphQL agents
- `JobTraceRunner`, which Bull, BullMQ and schedule share
- the instance decorator
- the outgoing span recorder
- `TracerService` and the stdout forwarder

A lint rule (`no-restricted-imports` in `.oxlintrc.json`) stops agents,
the instance decorator and the outgoing patches from importing the registry,
the buffer or the sampler again.

## Parity

| Suite                    | Before (master)                                    | After                                     |
| ------------------------ | -------------------------------------------------- | ----------------------------------------- |
| Unit                     | 739 passed, 1 skipped                              | 747 passed, 1 skipped (+8 recorder tests) |
| Integration (Redis only) | 35 files passed, 3 skipped; 268 passed, 58 skipped | identical                                 |
| Lint                     | 0 errors, 61 warnings                              | 0 errors, 52 warnings, none new           |

## How the seam differs from the Phase 0 sketch

- **Agents still create the async store.** It's the module's user-facing
  context: it's exported, and `setAttributes` values and the trace id live in
  it. So it outlives any one recorder. Only the registry's reserved keys
  (`#caller`, `#registryKey`) moved behind the recorder.
- **`ignore` stays in the agents; sampling moved into the recorder.** Agents
  pass `record: false` when their own `ignore` matched, plus a `sampling`
  tuple for `tracesSampleRate`.
- **`SpanRecorder` is an abstract class**, so Nest injects it by type.
- **`end()` takes a thunk.** The agents read `getUserId` on the deferred tick
  today, after guards have run, and still do.
- **`runManualSpan` returns a promise**, because the registry's manual spans
  always have.
- **`injectInto` and `carrier` are left for Phase 3.** Job enqueue still
  stamps the trace id from the store.

## Behaviour changes, all deliberate

1. **Unrecorded operations no longer fall back to their correlation id.**
   When an operation is ignored or sampled out, its store now points at an
   unregistered key (`#unrecorded`). Before, steps inside it looked the trace
   up by its trace id. If that id belonged to an operation still open in the
   process, the steps attached to it: an adopted `x-request-id` that is
   retried or fanned out, or a job inheriting its request's id. Jobs already
   guarded against this; now every protocol does. The
   `registry-span-recorder.spec.ts` test for this fails without the fix.
2. **RPC and WS get the duplicate-id handling HTTP already had.** If the id
   is already open, the operation gets a registry key of its own instead of
   overwriting the open trace.
3. **`abandon()` after `end()` is a no-op.** The aborted-request timer
   could fire after an operation ended. If a later request had reused the
   same `x-request-id` by then, the timer would drop that request's trace.
4. **Standalone GraphQL operations enter their store before sampling.** A
   sampled-out operation in a shared driver context used to inherit the
   previous operation's store, and could record into that trace. It now
   gets its own store, like every other agent.
5. **`captureError` on an unrecorded operation is silent**, rather than
   warning about a missing snapshot.
6. **HTTP redacts the URL before sampling**, so a sampled-out request also
   pays for the query redaction. It's a few regexes per request. If it shows
   up in Phase 5 benchmarks, make the start attributes lazy.

## Public API

- `createInstanceDecorator(spanRecorder, { skipInstrumentation })` replaces
  `(als, registry, { traceIdKey, skipInstrumentation })`. The registry was
  never exported, so no outside caller could pass one.
- `TraceSpanDelegate` keeps its `(id, name, tags)` constructor, and also
  accepts a `SpanTagSink`.

## Ready for Phase 2

An `OtelSpanRecorder` is a second `SpanRecorder` subclass. It can reuse
`registry-span-recorder.spec.ts` as a behavioural checklist. What changes is
how the assertions read the output, not the scenarios.
