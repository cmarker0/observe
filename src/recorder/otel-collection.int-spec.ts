import { Controller, Get, Injectable, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { ApolloDriver, ApolloDriverConfig } from "@nestjs/apollo";
import {
  Field,
  GraphQLModule,
  Int,
  ObjectType,
  Query,
  Resolver,
} from "@nestjs/graphql";
import * as api from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import {
  CollectedSnapshots,
  collectSnapshots,
  testObserveOptions,
} from "../testing/observe-harness.js";
import { ObserveAttributes } from "./otel-span-recorder.js";

const exporter = new InMemorySpanExporter();
const provider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
});

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: provider },
});

@ObjectType()
class Order {
  @Field(() => Int)
  id: number;
}

@Injectable()
class OrdersService {
  list(): Order[] {
    return [{ id: 1 }];
  }

  fail(): never {
    throw new Error("deliberate password=hunter2");
  }
}

@Controller()
class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  @Get("orders")
  findAll() {
    return this.orders.list();
  }

  @Get("boom")
  boom() {
    return this.orders.fail();
  }
}

@Resolver(() => Order)
class OrdersResolver {
  constructor(private readonly service: OrdersService) {}

  @Query(() => [Order])
  orders(): Order[] {
    return this.service.list();
  }

  @Query(() => Order)
  brokenOrder(): Order {
    return this.service.fail();
  }
}

@Module({
  imports: [
    ObserveModule.forRoot(testObserveOptions()),
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
      playground: false,
      includeStacktraceInErrorResponses: false,
    }),
  ],
  controllers: [OrdersController],
  providers: [OrdersService, OrdersResolver],
})
class OtelTestModule {}

/**
 * `createObserveModule({ opentelemetry })` end to end: the same agents as the
 * snapshot suites, recording spans through the OpenTelemetry API instead.
 */
describe("ObserveModule: OpenTelemetry recording", () => {
  let app: NestExpressApplication;
  let collected: CollectedSnapshots;

  const parentOf = (span: ReadableSpan) => span.parentSpanContext?.spanId;
  const idOf = (span: ReadableSpan) => span.spanContext().spanId;

  /** The finished spans of the one trace whose root is named `rootName`. */
  async function traceOf(rootName: string): Promise<ReadableSpan[]> {
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const root = exporter
        .getFinishedSpans()
        .find((span) => span.name === rootName);
      if (root) {
        const traceId = root.spanContext().traceId;
        return exporter
          .getFinishedSpans()
          .filter((span) => span.spanContext().traceId === traceId);
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error(
      `No "${rootName}" span. Finished: ${exporter
        .getFinishedSpans()
        .map((span) => span.name)
        .join(", ")}`,
    );
  }

  const named = (spans: ReadableSpan[], name: string) => {
    const found = spans.find((span) => span.name === name);
    expect(found, `span "${name}"`).toBeDefined();
    return found!;
  };

  beforeAll(async () => {
    api.context.setGlobalContextManager(
      new AsyncLocalStorageContextManager().enable(),
    );
    app = await NestFactory.create<NestExpressApplication>(OtelTestModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    collected = collectSnapshots(app);
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    api.context.disable();
  });

  beforeEach(() => {
    exporter.reset();
    collected.clear();
  });

  it("records an HTTP request as a SERVER span over its handler and providers", async () => {
    await request(app.getHttpServer()).get("/orders").expect(200);

    const spans = await traceOf("GET /orders");
    const root = named(spans, "GET /orders");
    expect(root.kind).toBe(api.SpanKind.SERVER);
    expect(root.attributes).toMatchObject({
      "http.request.method": "GET",
      "http.route": "/orders",
      "http.response.status_code": 200,
    });

    const handler = named(spans, "OrdersController.findAll");
    const listing = named(spans, "OrdersService.list");
    expect(parentOf(handler)).toBe(idOf(root));
    expect(parentOf(listing)).toBe(idOf(handler));
    // Spans go to OpenTelemetry, not to the collector.
    expect(collected.items).toHaveLength(0);
  });

  it("fails a request whose handler threw, with the error redacted", async () => {
    await request(app.getHttpServer()).get("/boom").expect(500);

    const spans = await traceOf("GET /boom");
    const root = named(spans, "GET /boom");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);

    const failing = named(spans, "OrdersService.fail");
    expect(failing.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(JSON.stringify(failing.events)).not.toContain("hunter2");
  });

  it("hangs resolvers and their providers under the GraphQL step of the request", async () => {
    await request(app.getHttpServer())
      .post("/graphql")
      .send({ query: "{ orders { id } }" })
      .expect(200);

    const spans = await traceOf("POST");
    const root = named(spans, "POST");
    expect(root.attributes).toMatchObject({
      "graphql.operation.type": "query",
      "graphql.operation.name": "orders",
    });

    const operation = named(spans, "OrdersResolver.orders");
    expect(parentOf(operation)).toBe(idOf(root));
    const listing = named(spans, "OrdersService.list");
    // The resolver method itself is also instrumented, so the provider sits
    // under it - and it under the GraphQL step.
    const chain = new Map(spans.map((span) => [idOf(span), span]));
    let cursor: ReadableSpan | undefined = listing;
    while (cursor && cursor !== operation) {
      cursor = chain.get(parentOf(cursor) ?? "");
    }
    expect(cursor).toBe(operation);
  });

  it("fails a GraphQL request whose resolver threw, though it answered 200", async () => {
    await request(app.getHttpServer())
      .post("/graphql")
      .send({ query: "{ brokenOrder { id } }" })
      .expect(200);

    const spans = await traceOf("POST");
    const root = named(spans, "POST");
    expect(root.attributes["http.response.status_code"]).toBe(200);
    expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
  });
});
