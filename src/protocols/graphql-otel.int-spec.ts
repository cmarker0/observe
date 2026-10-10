import { ConflictException, Injectable, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { ApolloDriver, ApolloDriverConfig } from "@nestjs/apollo";
import {
  Args,
  Context,
  Field,
  GraphQLModule,
  Int,
  Mutation,
  ObjectType,
  Parent,
  Query,
  ResolveField,
  Resolver,
} from "@nestjs/graphql";
import * as api from "@opentelemetry/api";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import request from "supertest";
import { createObserveModule } from "../observe.module.js";
import { ObserveAttributes } from "../recorder/otel-span-recorder.js";
import {
  installOtelGlobals,
  OtelTestSpans,
  spanNamed,
  spanTree,
  uninstallOtelGlobals,
} from "../testing/otel-harness.js";

const spans = new OtelTestSpans();

const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: { tracerProvider: spans.provider },
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

@ObjectType()
class Customer {
  @Field(() => Int)
  id: number;
}

@ObjectType()
class Order {
  @Field(() => Int)
  id: number;

  @Field()
  name: string;
}

@Injectable()
class OrdersService {
  list(): Order[] {
    return [
      { id: 1, name: "first" },
      { id: 2, name: "second" },
    ];
  }

  find(id: number): Order {
    return { id, name: `order-${id}` };
  }

  create(name: string): Order {
    return { id: 99, name };
  }

  async failLater(): Promise<never> {
    await sleep(10);
    throw new Error("rejected after a wait");
  }
}

@Injectable()
class PricingService {
  /** Async on purpose: the span has to end in the right trace after a tick. */
  async quote(id: number): Promise<number> {
    await sleep(5);
    return id * 10;
  }

  discount(): number {
    throw new Error("no discount for you");
  }
}

@Injectable()
class CustomersService {
  byIds(ids: number[]): Customer[] {
    return ids.map((id) => ({ id }));
  }
}

/**
 * A DataLoader in miniature: `load` queues a key, and the queue is flushed on
 * the next macrotask with one `byIds` call. The flush runs in whatever context
 * the first `load` ran in - the shape that most often puts batched work under
 * the wrong parent, or in the wrong request.
 */
class CustomerLoader {
  private pending: { id: number; resolve: (customer: Customer) => void }[] = [];

  constructor(private readonly customers: CustomersService) {}

  load(id: number): Promise<Customer> {
    return new Promise((resolve) => {
      if (this.pending.length === 0) {
        setImmediate(() => this.flush());
      }
      this.pending.push({ id, resolve });
    });
  }

  private flush() {
    const batch = this.pending;
    this.pending = [];
    const found = this.customers.byIds(batch.map(({ id }) => id));
    batch.forEach(({ resolve }, index) => resolve(found[index]));
  }
}

@Resolver(() => Order)
class OrdersResolver {
  constructor(
    private readonly orders: OrdersService,
    private readonly pricing: PricingService,
    private readonly customers: CustomersService,
  ) {}

  @Query(() => [Order])
  allOrders(): Order[] {
    return this.orders.list();
  }

  /** A schema name that differs from the method name. */
  @Query(() => [Order], { name: "latestOrders" })
  findLatest(): Order[] {
    return this.orders.list();
  }

  /** Waits before and calls a provider after, so concurrent calls interleave. */
  @Query(() => Order)
  async slowOrder(
    @Args("id", { type: () => Int }) id: number,
    @Args("delayMs", { type: () => Int }) delayMs: number,
  ): Promise<Order> {
    await sleep(delayMs);
    return this.orders.find(id);
  }

  @Query(() => Order)
  rejectLater(): Promise<Order> {
    return this.orders.failLater();
  }

  @Query(() => Order)
  conflictingOrder(): Order {
    throw new ConflictException("already taken");
  }

  @Mutation(() => Order)
  createOrder(@Args("name") name: string): Order {
    return this.orders.create(name);
  }

  /** Field resolvers get no span of their own; the providers they call do. */
  @ResolveField(() => Int)
  total(@Parent() order: Order): Promise<number> {
    return this.pricing.quote(order.id);
  }

  @ResolveField(() => Int, { nullable: true })
  discount(): number {
    return this.pricing.discount();
  }

  /** One loader per GraphQL context, i.e. per operation, as DataLoader is used. */
  @ResolveField(() => Customer)
  customer(
    @Parent() order: Order,
    @Context() context: { customerLoader?: CustomerLoader },
  ): Promise<Customer> {
    context.customerLoader ??= new CustomerLoader(this.customers);
    return context.customerLoader.load(order.id);
  }
}

@Module({
  imports: [
    ObserveModule.forRoot({}),
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
      playground: false,
      includeStacktraceInErrorResponses: false,
      // Several operations in one HTTP request, executed concurrently.
      allowBatchedHttpRequests: true,
    }),
  ],
  providers: [OrdersResolver, OrdersService, PricingService, CustomersService],
})
class GraphqlOtelTestModule {}

/** Every finished span, grouped by trace and drawn as a tree per trace. */
function treesByTrace(finished: ReadableSpan[]): Map<string, string> {
  const traces = new Map<string, ReadableSpan[]>();
  for (const span of finished) {
    const { traceId } = span.spanContext();
    traces.set(traceId, [...(traces.get(traceId) ?? []), span]);
  }
  return new Map(
    [...traces].map(([traceId, members]) => [traceId, spanTree(members)]),
  );
}

const rootOf = (members: ReadableSpan[]) =>
  members.find((span) => span.kind === api.SpanKind.SERVER)!;

/**
 * GraphQL collection recorded as OpenTelemetry spans - the counterpart of
 * `graphql-collection.int-spec.ts`.
 *
 * Over HTTP the operation's root is the HTTP SERVER span, named `POST` for
 * want of a route, and the GraphQL agent labels it with `graphql.operation.*`
 * and the sanitised document. Beneath it sits one INTERNAL step per operation,
 * named after the resolver method that serves its first root field; every
 * provider the operation reaches - from the root resolver, from field
 * resolvers, from batched loaders - nests under that step.
 *
 * The driver's hooks return into execution rather than wrapping it, so the
 * step is made current by mutating the recorder's context cell instead of
 * through `context.with`. That is where spans can land under the wrong parent
 * or in the wrong request, so most of what follows is about the tree's shape
 * under concurrency, async work and errors, asserted whole per trace.
 *
 * The basic query and the synchronously failing resolver are covered in
 * `otel-collection.int-spec.ts` and not repeated here.
 */
describe("ObserveModule: GraphQL over OpenTelemetry", () => {
  let app: NestExpressApplication;

  const gql = (query: string) =>
    request(app.getHttpServer()).post("/graphql").send({ query });

  beforeAll(async () => {
    installOtelGlobals();
    app = await NestFactory.create<NestExpressApplication>(
      GraphqlOtelTestModule,
      { instrument: ObserveInstrument, logger: false },
    );
    await app.listen(0, "127.0.0.1");
  });

  afterAll(async () => {
    await app?.close();
    uninstallOtelGlobals();
  });

  beforeEach(() => spans.reset());

  it("names the step after the method serving a renamed field, and the operation after the schema field", async () => {
    // `@Query({ name })` decouples the schema field from the method: the
    // operation attributes speak the schema's language, the step the code's.
    await gql("{ latestOrders { id } }").expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.findLatest",
        "    INTERNAL OrdersService.list",
      ].join("\n"),
    );
    expect(spanNamed(trace, "POST").attributes).toMatchObject({
      "graphql.operation.type": "query",
      "graphql.operation.name": "latestOrders",
      [ObserveAttributes.OPERATION_ID]: "Query.latestOrders",
      "graphql.document": "{ latestOrders { id } }",
      "http.request.method": "POST",
      "url.path": "/graphql",
    });
  });

  it("records a named mutation with its document sanitised", async () => {
    await gql(
      'mutation CreateOrder { createOrder(name: "confidential") { id } }',
    ).expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.createOrder",
        "    INTERNAL OrdersService.create",
      ].join("\n"),
    );
    const root = spanNamed(trace, "POST");
    // `graphql.operation.name` is the root field, not the document's
    // operation name (see phase-2-notes.md); the name stays in the document,
    // the inline literal does not.
    expect(root.attributes).toMatchObject({
      "graphql.operation.type": "mutation",
      "graphql.operation.name": "createOrder",
      "graphql.document":
        "mutation CreateOrder { createOrder(name: _) { id } }",
    });
    expect(JSON.stringify(trace.map((span) => span.attributes))).not.toContain(
      "confidential",
    );
  });

  it("nests async field-resolver work under the operation step, with no span per field", async () => {
    const response = await gql("{ allOrders { id total } }").expect(200);
    expect(response.body.data.allOrders).toEqual([
      { id: 1, total: 10 },
      { id: 2, total: 20 },
    ]);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL PricingService.quote",
        "    INTERNAL PricingService.quote",
      ].join("\n"),
    );
  });

  it("covers the whole of an async root resolver, and the provider it calls after waiting", async () => {
    await gql("{ slowOrder(id: 7, delayMs: 40) { id total } }").expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
        "    INTERNAL PricingService.quote",
      ].join("\n"),
    );
    const step = spanNamed(trace, "OrdersResolver.slowOrder");
    const durationMs = step.duration[0] * 1e3 + step.duration[1] / 1e6;
    expect(durationMs).toBeGreaterThanOrEqual(35);
  });

  it("gives a multi-field document one step, named after its first field, over both fields' work", async () => {
    await gql(
      "{ allOrders { id } slowOrder(id: 3, delayMs: 5) { id } }",
    ).expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL OrdersService.find",
      ].join("\n"),
    );
  });

  it("nests a batched loader's single call under the operation step", async () => {
    const response = await gql("{ allOrders { id customer { id } } }").expect(
      200,
    );
    expect(response.body.data.allOrders[1].customer).toEqual({ id: 2 });

    const trace = await spans.traceOf("POST");
    // Two `customer` fields, one batched lookup.
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL CustomersService.byIds",
      ].join("\n"),
    );
  });

  it("fails the operation when an async root resolver rejects, though it answered 200", async () => {
    const response = await gql("{ rejectLater { id } }").expect(200);
    expect(response.body.errors[0].message).toBe("rejected after a wait");

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.rejectLater",
        "    INTERNAL OrdersService.failLater",
      ].join("\n"),
    );
    const failing = spanNamed(trace, "OrdersService.failLater");
    expect(failing.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(failing.events[0]?.attributes?.["exception.message"]).toBe(
      "rejected after a wait",
    );

    const step = spanNamed(trace, "OrdersResolver.rejectLater");
    expect(step.status.code).toBe(api.SpanStatusCode.ERROR);
    const root = spanNamed(trace, "POST");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes).toMatchObject({
      "http.response.status_code": 200,
      [ObserveAttributes.STATUS_CODE]: 500,
      [ObserveAttributes.ERROR_HANDLED]: false,
      "error.type": "Error",
    });
  });

  it("fails the operation when a field resolver throws, keeping the partial data", async () => {
    // The root resolver succeeded; only a nested field failed. GraphQL still
    // answers with data and errors side by side, and the operation is failed
    // through the step, which ends with the first error the driver reports.
    const response = await gql("{ allOrders { id discount } }").expect(200);
    expect(response.body.data.allOrders).toEqual([
      { id: 1, discount: null },
      { id: 2, discount: null },
    ]);
    expect(response.body.errors).toHaveLength(2);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL PricingService.discount",
        "    INTERNAL PricingService.discount",
      ].join("\n"),
    );
    for (const span of trace.filter(
      (candidate) => candidate.name === "PricingService.discount",
    )) {
      expect(span.status.code).toBe(api.SpanStatusCode.ERROR);
    }
    expect(spanNamed(trace, "OrdersService.list").status.code).toBe(
      api.SpanStatusCode.UNSET,
    );
    expect(spanNamed(trace, "OrdersResolver.allOrders").status).toMatchObject({
      code: api.SpanStatusCode.ERROR,
      message: "no discount for you",
    });
    const root = spanNamed(trace, "POST");
    expect(root.status.code).toBe(api.SpanStatusCode.ERROR);
    expect(root.attributes[ObserveAttributes.STATUS_CODE]).toBe(500);
  });

  it("classifies an intrinsic exception as handled, under its own 4xx, without failing the HTTP span", async () => {
    await gql("{ conflictingOrder { id } }").expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      ["SERVER POST", "  INTERNAL OrdersResolver.conflictingOrder"].join("\n"),
    );
    const root = spanNamed(trace, "POST");
    expect(root.attributes).toMatchObject({
      "http.response.status_code": 200,
      [ObserveAttributes.STATUS_CODE]: 409,
      [ObserveAttributes.ERROR_HANDLED]: true,
    });
    // Over HTTP a 4xx is the client's failure: semconv leaves it unset.
    expect(root.status.code).toBe(api.SpanStatusCode.UNSET);
    expect(
      spanNamed(trace, "OrdersResolver.conflictingOrder").status.code,
    ).toBe(api.SpanStatusCode.ERROR);
  });

  it("names the step after the schema field when no resolver serves it", async () => {
    // A document that fails validation still parses, so the step opens; no
    // class registered `Query.nope`, so the schema type stands in.
    await gql("{ nope }").expect(400);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      ["SERVER POST", "  INTERNAL Query.nope"].join("\n"),
    );
    expect(spanNamed(trace, "POST").attributes).toMatchObject({
      "http.response.status_code": 400,
      [ObserveAttributes.OPERATION_ID]: "Query.nope",
    });
  });

  it("keeps concurrent operations' spans in their own traces", async () => {
    // Started together, finishing in reverse order, each calling providers
    // both before and after it yields - including a failing field and a
    // batched loader. Any span recorded against the wrong cell shows up as an
    // extra or missing line in some trace's tree.
    const queries = [
      "{ slowOrder(id: 1, delayMs: 60) { id total customer { id } } }",
      "{ slowOrder(id: 2, delayMs: 40) { id total customer { id } } }",
      "{ slowOrder(id: 3, delayMs: 20) { id discount } }",
      "{ allOrders { id total customer { id } } }",
      'mutation { createOrder(name: "parallel") { id total } }',
      "{ slowOrder(id: 6, delayMs: 0) { id customer { id } } }",
    ];
    const responses = await Promise.all(queries.map((query) => gql(query)));
    for (const response of responses) {
      expect(response.status).toBe(200);
    }

    const roots = await spans.waitFor(
      (finished) => {
        const found = finished.filter((span) => span.name === "POST");
        return found.length === queries.length ? found : undefined;
      },
      3000,
      `${queries.length} POST spans`,
    );
    const trees = treesByTrace(spans.finished);
    expect(trees.size).toBe(queries.length);

    const expected: Record<string, string[]> = {
      "{ slowOrder(id: _, delayMs: _) { id total customer { id } } }": [
        "SERVER POST",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
        "    INTERNAL PricingService.quote",
        "    INTERNAL CustomersService.byIds",
      ],
      "{ slowOrder(id: _, delayMs: _) { id discount } }": [
        "SERVER POST",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
        "    INTERNAL PricingService.discount",
      ],
      "{ allOrders { id total customer { id } } }": [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL PricingService.quote",
        "    INTERNAL PricingService.quote",
        "    INTERNAL CustomersService.byIds",
      ],
      "mutation { createOrder(name: _) { id total } }": [
        "SERVER POST",
        "  INTERNAL OrdersResolver.createOrder",
        "    INTERNAL OrdersService.create",
        "    INTERNAL PricingService.quote",
      ],
      "{ slowOrder(id: _, delayMs: _) { id customer { id } } }": [
        "SERVER POST",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
        "    INTERNAL CustomersService.byIds",
      ],
    };
    for (const root of roots) {
      const { traceId } = root.spanContext();
      const document = String(root.attributes["graphql.document"]);
      expect(expected[document], document).toBeDefined();
      // `quote` and `byIds` race within an operation; the order among
      // siblings is not what this asserts.
      const lines = (tree: string[]) => [...tree].sort();
      expect(lines(trees.get(traceId)!.split("\n")), document).toEqual(
        lines(expected[document]),
      );
      // Only the operation whose field failed is failed.
      const failed = document.includes("discount");
      expect(root.status.code, document).toBe(
        failed ? api.SpanStatusCode.ERROR : api.SpanStatusCode.UNSET,
      );
    }
  });

  it("keeps two concurrent operations apart when they share an x-request-id", async () => {
    // A fanning-out caller sends its id twice. Each request is still its own
    // trace; the shared id is only the correlation attribute.
    await Promise.all([
      gql("{ slowOrder(id: 1, delayMs: 30) { id total } }")
        .set("x-request-id", "shared-trace-1")
        .expect(200),
      gql("{ slowOrder(id: 2, delayMs: 10) { id total } }")
        .set("x-request-id", "shared-trace-1")
        .expect(200),
    ]);

    const roots = await spans.waitFor((finished) => {
      const found = finished.filter((span) => span.name === "POST");
      return found.length === 2 ? found : undefined;
    });
    const trees = treesByTrace(spans.finished);
    expect(trees.size).toBe(2);
    for (const root of roots) {
      expect(root.attributes[ObserveAttributes.CORRELATION_ID]).toBe(
        "shared-trace-1",
      );
      expect(trees.get(root.spanContext().traceId)).toBe(
        [
          "SERVER POST",
          "  INTERNAL OrdersResolver.slowOrder",
          "    INTERNAL OrdersService.find",
          "    INTERNAL PricingService.quote",
        ].join("\n"),
      );
    }
  });

  it("opens a GraphQL SERVER root per operation when no transport is in front, even concurrently", async () => {
    // Executed straight on the Apollo server, so no HTTP span exists and the
    // agent opens the operation itself, installing its store with
    // `enterWith` - the path the agent documents as best effort.
    const apollo = (app.get(GraphQLModule).graphQlAdapter as ApolloDriver)
      .instance;
    await Promise.all([
      apollo.executeOperation({
        query: "{ slowOrder(id: 1, delayMs: 30) { id total } }",
      }),
      apollo.executeOperation({
        query: "{ allOrders { id customer { id } } }",
      }),
    ]);

    const slow = await spans.traceOf("Query.slowOrder");
    const all = await spans.traceOf("Query.allOrders");
    expect(spanTree(slow)).toBe(
      [
        "SERVER Query.slowOrder",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
        "    INTERNAL PricingService.quote",
      ].join("\n"),
    );
    expect(spanTree(all)).toBe(
      [
        "SERVER Query.allOrders",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "    INTERNAL CustomersService.byIds",
      ].join("\n"),
    );
    expect(rootOf(slow).attributes).toMatchObject({
      [ObserveAttributes.PROTOCOL]: "graphql",
      "graphql.operation.type": "query",
      "graphql.operation.name": "slowOrder",
    });
    expect(spans.finished.some((span) => span.name === "POST")).toBe(false);
  });

  // Operations batched into one HTTP request enter their steps from the same
  // request context. Each step must open under the operation and become
  // current only for its own operation - not by swapping the span in a cell
  // the batch shares, which nested the second step under the first and put
  // each operation's providers under whichever step entered last. (The
  // registry recorder still has that flaw: it shares CALLER_METADATA_KEY.)
  it("keeps operations batched into one HTTP request side by side under it", async () => {
    // The second operation stays open for 40ms, while the first calls its
    // providers straight away.
    await request(app.getHttpServer())
      .post("/graphql")
      .send([
        { query: "{ allOrders { id } }" },
        { query: "{ slowOrder(id: 5, delayMs: 40) { id } }" },
      ])
      .expect(200);

    const trace = await spans.traceOf("POST");
    expect(spanTree(trace)).toBe(
      [
        "SERVER POST",
        "  INTERNAL OrdersResolver.allOrders",
        "    INTERNAL OrdersService.list",
        "  INTERNAL OrdersResolver.slowOrder",
        "    INTERNAL OrdersService.find",
      ].join("\n"),
    );
  });
});
