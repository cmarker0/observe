import { join } from "node:path";
import {
  BadRequestException,
  Controller,
  INestMicroservice,
  Injectable,
  IntrinsicException,
  Module,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { GrpcMethod, Transport } from "@nestjs/microservices";
import { credentials, loadPackageDefinition, Metadata } from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import * as api from "@opentelemetry/api";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import { freePort } from "../testing/observe-harness.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  parentIdOf,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const spans = new OtelTestSpans();

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

const PROTO_PATH = join(
  import.meta.dirname,
  "..",
  "testing",
  "orders.test.proto",
);

/** A caller's context, as an upstream service would have sent it. */
const CALLER_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN_ID = "00f067aa0ba902b7";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** See `grpc-collection.int-spec.ts`: a failure modelled without HTTP. */
class DeclinedException extends IntrinsicException {}

@Injectable()
class OrdersService {
  find(id: string) {
    return { id, name: `order-${id}` };
  }
}

@Controller()
class OrdersGrpcController {
  constructor(private readonly orders: OrdersService) {}

  @GrpcMethod("Orders", "FindOne")
  findOne(data: { id: string }) {
    return this.orders.find(data.id);
  }

  @GrpcMethod("Orders", "Ping")
  ping(data: { id: string }) {
    return { id: data.id, name: "pong" };
  }

  @GrpcMethod("Orders", "Explode")
  explode(): never {
    throw new Error("deliberate password=hunter2");
  }

  @GrpcMethod("Orders", "Reject")
  reject(): never {
    throw new BadRequestException("rejected");
  }

  @GrpcMethod("Orders", "Decline")
  decline(): never {
    throw new DeclinedException("declined");
  }

  /** Async failures take a different path through Nest's gRPC server. */
  @GrpcMethod("Orders", "ExplodeAsync")
  async explodeAsync(): Promise<never> {
    await new Promise((resolve) => setTimeout(resolve, 5));
    throw new Error("deliberate");
  }

  @GrpcMethod("Orders", "RejectAsync")
  async rejectAsync(): Promise<never> {
    await new Promise((resolve) => setTimeout(resolve, 5));
    throw new BadRequestException("rejected");
  }
}

@Module({
  imports: [ObserveModule.forRoot({})],
  controllers: [OrdersGrpcController],
  providers: [OrdersService],
})
class GrpcTestModule {}

/**
 * `grpc-collection.int-spec.ts` with OpenTelemetry recording.
 *
 * A unary call becomes a SERVER span named after the gRPC method, with the
 * handler and the providers it calls beneath it. The agent's gRPC branch
 * (`startGrpcRequestTracing`) is separate code from the message transports',
 * and is the one that hands the recorder gRPC `Metadata` - read through
 * `getMap()` - as the carrier a caller's `traceparent` arrives in.
 */
describe("ObserveModule with OpenTelemetry: gRPC", () => {
  let app: INestMicroservice;
  let client: any;

  beforeAll(async () => {
    installOtelGlobals();
    const url = `127.0.0.1:${await freePort()}`;

    app = await NestFactory.createMicroservice(GrpcTestModule, {
      transport: Transport.GRPC,
      options: { package: "orderstest", protoPath: PROTO_PATH, url },
      instrument: ObserveInstrument,
      logger: false,
    } as never);
    await app.listen();

    // A raw grpc-js client: nothing on the caller's side is recorded, so
    // every span is the server's.
    const definition = loadSync(PROTO_PATH, {
      keepCase: true,
      defaults: true,
      oneofs: true,
    });
    const proto = loadPackageDefinition(definition) as any;
    client = new proto.orderstest.Orders(url, credentials.createInsecure());
  });

  afterAll(async () => {
    client?.close?.();
    await app?.close();
    uninstallOtelGlobals();
  });

  beforeEach(() => spans.reset());

  const call = (
    method: string,
    payload: Record<string, unknown>,
    metadata = new Metadata(),
  ) =>
    new Promise<any>((resolve, reject) => {
      client[method](payload, metadata, (error: unknown, response: unknown) =>
        error ? reject(error) : resolve(response),
      );
    });

  const withTraceparent = (traceparent: string) => {
    const metadata = new Metadata();
    metadata.set("traceparent", traceparent);
    return metadata;
  };

  /** The SERVER span named `name`, once it has ended. */
  const operationNamed = (name: string) =>
    spans.waitFor(
      (finished) =>
        finished.find(
          (span) => span.name === name && span.kind === api.SpanKind.SERVER,
        ),
      3000,
      `a "${name}" SERVER span`,
    );

  it("records a unary call as a SERVER span over its handler and providers", async () => {
    await expect(call("FindOne", { id: "42" })).resolves.toMatchObject({
      id: "42",
      name: "order-42",
    });

    const trace = await spans.traceOf("orderstest.Orders/FindOne");
    expect(spanTree(trace)).toBe(
      [
        "SERVER orderstest.Orders/FindOne",
        "  INTERNAL OrdersGrpcController.findOne",
        "    INTERNAL OrdersService.find",
      ].join("\n"),
    );

    const root = spanNamed(trace, "orderstest.Orders/FindOne");
    // No context in the metadata, so the call starts a trace of its own.
    expect(root.parentSpanContext).toBeUndefined();
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    expect(root.attributes).toMatchObject({
      "rpc.system": "grpc",
      "rpc.method": "FindOne",
      [ObserveAttributes.PROTOCOL]: "GRPC",
      [ObserveAttributes.OPERATION_ID]: "FindOne",
    });
    expect(root.attributes[ObserveAttributes.CORRELATION_ID]).toMatch(UUID);
  });

  /**
   * Semantic conventions name a gRPC server span `$package.$service/$method`
   * and give it `rpc.service`, so two services with a method of the same
   * name stay apart. Nest passes on only the method; the service comes from
   * the call's own path (`/orderstest.Orders/FindOne`).
   */
  it("names the service the method belongs to", async () => {
    await call("FindOne", { id: "1" });

    const root = await spans.waitFor((finished) =>
      finished.find((span) => span.kind === api.SpanKind.SERVER),
    );
    expect(root.name).toBe("orderstest.Orders/FindOne");
    expect(root.attributes).toMatchObject({
      "rpc.system": "grpc",
      "rpc.service": "orderstest.Orders",
      "rpc.method": "FindOne",
    });
  });

  it("starts one trace per call, named after each service method", async () => {
    await call("FindOne", { id: "1" });
    await call("Ping", { id: "1" });
    await call("FindOne", { id: "2" });

    const roots = await spans.waitFor((finished) => {
      const found = finished.filter(
        (span) => span.kind === api.SpanKind.SERVER,
      );
      return found.length >= 3 ? found : undefined;
    });
    expect(roots.map((span) => span.name).sort()).toEqual([
      "orderstest.Orders/FindOne",
      "orderstest.Orders/FindOne",
      "orderstest.Orders/Ping",
    ]);
    expect(new Set(roots.map((span) => span.spanContext().traceId)).size).toBe(
      3,
    );
  });

  /**
   * A handler that throws produces an observable that errors rather than
   * completes; the trace has to end on that path too (Nest fires the
   * processing-end hook from `error` since @nestjs/microservices 11.1.29).
   */
  it("fails a call whose handler threw, with the error redacted", async () => {
    await expect(call("Explode", { id: "9" })).rejects.toBeDefined();

    const trace = await spans.traceOf("orderstest.Orders/Explode");
    expect(spanTree(trace)).toBe(
      [
        "SERVER orderstest.Orders/Explode",
        "  INTERNAL OrdersGrpcController.explode",
      ].join("\n"),
    );

    const root = spanNamed(trace, "orderstest.Orders/Explode");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 500,
      [ObserveAttributes.ERROR_HANDLED]: false,
      "error.type": "Error",
    });

    const handler = spanNamed(trace, "OrdersGrpcController.explode");
    expect(handler.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(handler.events[0].attributes?.["exception.message"]).toBe(
      "deliberate password=[REDACTED]",
    );
  });

  it("marks a deliberately raised failure as handled", async () => {
    await expect(call("Reject", { id: "9" })).rejects.toBeDefined();

    // Filed under a 4xx and marked handled, so the backend counts it apart
    // from a crash; it still fails the span, as an RPC has no client to blame.
    const root = await operationNamed("orderstest.Orders/Reject");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 400,
      [ObserveAttributes.ERROR_HANDLED]: true,
      "error.type": "BadRequestException",
    });
  });

  it("marks a bare IntrinsicException as handled too", async () => {
    await expect(call("Decline", { id: "9" })).rejects.toBeDefined();

    const root = await operationNamed("orderstest.Orders/Decline");
    expect(root.attributes).toMatchObject({
      [ObserveAttributes.STATUS_CODE]: 400,
      [ObserveAttributes.ERROR_HANDLED]: true,
      "error.type": "DeclinedException",
    });
  });

  it("classifies an async handler's failures the same way", async () => {
    await expect(call("ExplodeAsync", { id: "9" })).rejects.toBeDefined();
    await expect(call("RejectAsync", { id: "9" })).rejects.toBeDefined();

    const crashed = await operationNamed("orderstest.Orders/ExplodeAsync");
    const rejected = await operationNamed("orderstest.Orders/RejectAsync");
    expect(crashed.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(crashed.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
    expect(rejected.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(rejected.attributes[ObserveAttributes.STATUS_CODE]).toBe(400);
  });

  it("continues the trace a caller sent in the call's metadata", async () => {
    await call(
      "FindOne",
      { id: "5" },
      withTraceparent(`00-${CALLER_TRACE_ID}-${CALLER_SPAN_ID}-01`),
    );

    const trace = await spans.traceOf("orderstest.Orders/FindOne");
    const root = spanNamed(trace, "orderstest.Orders/FindOne");
    expect(root.spanContext().traceId).toBe(CALLER_TRACE_ID);
    expect(parentIdOf(root)).toBe(CALLER_SPAN_ID);
    // The caller's span is not here, so the call is still this trace's root.
    expect(spanTree(trace)).toBe(
      [
        "SERVER orderstest.Orders/FindOne",
        "  INTERNAL OrdersGrpcController.findOne",
        "    INTERNAL OrdersService.find",
      ].join("\n"),
    );
  });

  /**
   * The caller's sampling decision travels with its context: under the SDK's
   * default parent-based sampler, a call whose caller sampled out records
   * nothing.
   */
  it("records nothing for a caller that sampled the trace out", async () => {
    await call(
      "FindOne",
      { id: "6" },
      withTraceparent(`00-${CALLER_TRACE_ID}-${CALLER_SPAN_ID}-00`),
    );
    // A sampled call after it, to know the first has had time to finish.
    await call("Ping", { id: "6" });
    await operationNamed("orderstest.Orders/Ping");

    expect(
      spans.finished.filter(
        (span) => span.spanContext().traceId === CALLER_TRACE_ID,
      ),
    ).toEqual([]);
    expect(spans.finished.map((span) => span.name)).not.toContain(
      "orderstest.Orders/FindOne",
    );
  });

  it("starts a trace of its own when the metadata holds no valid context", async () => {
    await call("FindOne", { id: "7" }, withTraceparent("00-not-a-trace-01"));

    const root = await operationNamed("orderstest.Orders/FindOne");
    expect(root.parentSpanContext).toBeUndefined();
    expect(root.spanContext().traceId).toMatch(/^[0-9a-f]{32}$/);
  });
});
