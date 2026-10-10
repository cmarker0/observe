# OTel migration: semantic-convention gaps

Closes the gaps Phase 5 listed (`phase-5-notes.md`, "Semantic-convention
gaps"). Every change is to OTel mode; the default (registry) mode records
what it did before, apart from the `ClientProxy` exclusion, which applies to
both.

## Fixed

| Gap                                                                         | Now                                                                                                                                                                                                                           |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Job roots had no `messaging.system`                                         | `bullmq` (BullMQ and BullMQ Pro) and `bull`. The queue agents pass the driver (`JobRunDescriptor.system` → `OperationStart.messagingSystem`). `@nestjs/schedule` runs have none, since no broker delivered them              |
| gRPC SERVER spans had no `rpc.grpc.status_code`                             | Read from the error that escaped the handler: `RpcException({ code })` or a numeric `code` gives that code, any other error `UNKNOWN` (2), as grpc-js sends it, and success `OK` (0)                                        |
| `graphql.operation.name` held the root field (`createOrder`)                | It holds the document's operation name (`CreateOrder`), and is absent for an anonymous operation. The root field stays in `nestjs.observe.operation_id` (`Mutation.createOrder`)                                             |
| A bull `done(err)` was `error.type: "failed"` with no exception event       | The error reaches the run's root as an `exception` event, with `error.type` set to its class and the status message set. The runner's `settle` carries the error (`OperationEnd.error`). A thrown error stays on the step that threw it and is not recorded twice |
| `ClientProxy` internals (`ClientTCP.connect`, `createPacket`, …) were spans | Every `ClientProxy`, built-in or custom, is skipped by the instance decorator, matched with `instanceof` against the lazily loaded class. The microservice's SERVER span now hangs straight from the handler that sent the message |
| A GraphQL document with a syntax error opened a step for its first field   | Documents are checked with `graphql`'s own `parse`, cached per document like the scan's labels, before a step opens. A document that does not parse opens no step and does not annotate the operation                       |

### gRPC span status

`rpc.grpc.status_code` also decides the span status now, per semantic
conventions for SERVER spans:

- Only `UNKNOWN`, `DEADLINE_EXCEEDED`, `UNIMPLEMENTED`, `INTERNAL`,
  `UNAVAILABLE` and `DATA_LOSS` set it to ERROR.
- A code that answers the request, such as `NOT_FOUND` or
  `INVALID_ARGUMENT`, leaves the status unset, as a 4xx does over HTTP.

A thrown `BadRequestException` is not an `RpcException`. Nest sends it with
no code, the client sees `UNKNOWN`, and the span is still ERROR. Throw
`RpcException({ code })` to answer with a client-side code.

The limit: Nest's end hook is given only the request, so the code is
inferred, not observed. An error raised outside any instrumented step
(before the handler, by something not instrumented) reads as `OK`.

## Not fixed

- **The registry recorder still mis-parents batched GraphQL operations.** A
  fix would give each operation its own copy of the request's async store.
  Values the application sets on that store during an operation would then
  stop reaching the request. That is a behaviour change to the default mode
  for a recorder the major release removes.
- **GraphQL span names** stay `Query.orders` and the request's
  `POST /graphql`, not semantic conventions' `query RecentOrders`. The
  operation id is what operations are grouped by.
- **Multi-operation documents** are still labelled from their first
  operation, even when the request's `operationName` selects another.

## Tests

Each new or changed assertion was mutation-checked: 16 mutations, all
caught.

- `otel-span-recorder.spec.ts`: `messaging.system`, the document's operation
  name, a driver-reported error and no double recording, and gRPC status
  codes (OK, an `RpcException` client code, a server code, no code).
- `jobs-otel`: `messaging.system` for BullMQ and bull and none for schedule,
  a callback processor's `done(err)` on the run, and a thrown error not
  repeated on the run.
- `grpc-otel`: status codes checked against what the client actually
  received (`OK`, `UNKNOWN`, `NOT_FOUND` unset, `UNAVAILABLE` ERROR). Two
  methods were added to `orders.test.proto` for this.
- `graphql-otel` and `otel-collection`: the named and anonymous operation
  name, and no step for a syntax error.
- `microservice-otel`: no `LoopbackClient.*` spans, with the server hanging
  directly under the handler.
- `graphql-operation-parser.spec.ts`: `createSyntaxCheck` rejects what the
  scan would still label, and parses each document once.

## Verification

- Unit: **796 passed**, 1 skipped (was 788).
- Integration (Redis): 46 files passed and 4 skipped. **393 passed**, 62
  skipped (was 390).
- `npm run test:otel-collector`: 2 passed.
- Lint: 0 errors and 52 warnings, unchanged. Typecheck and build are clean.
