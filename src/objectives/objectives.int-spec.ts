import {
  Controller,
  Get,
  INestApplication,
  Module,
  Post,
  VersioningType,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { createServer, Server } from "node:http";
import { gunzipSync } from "node:zlib";
import { createObserveModule } from "../observe.module.js";
import { freePort, waitFor } from "../testing/observe-harness.js";
import { Objective } from "./objective.decorator.js";

@Controller({ path: "orders", version: "1" })
class OrdersController {
  @Post()
  @Objective({ availability: 99.9, latency: { underMs: 300, target: 99 } })
  create() {
    return { created: true };
  }

  @Get()
  list() {
    return [];
  }
}

/**
 * `@Objective` end to end: declared on a real controller, paired with the
 * route a real request took - global prefix and URI version included, which
 * is the point of pairing at request time - and delivered to a collector in
 * the batch root.
 */
describe("@Objective: declarations reach the collector", () => {
  let app: INestApplication;
  let baseUrl: string;
  let collector: Server;
  const batches: Array<{ objectives?: Array<Record<string, unknown>> }> = [];

  const declarations = () => batches.flatMap((batch) => batch.objectives ?? []);

  beforeAll(async () => {
    collector = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        batches.push(JSON.parse(gunzipSync(Buffer.concat(chunks)).toString()));
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      });
    });
    const collectorPort = await freePort();
    await new Promise<void>((resolve) =>
      collector.listen(collectorPort, resolve),
    );

    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [
        ObserveModule.forRoot({
          appKey: "test-key",
          appSecret: "test-secret",
          serviceId: "objectives-app",
          serviceVersion: "objectives-int",
          endpoint: `http://127.0.0.1:${collectorPort}`,
          flushInterval: 1000,
          runtimeMetrics: false,
          forwardLogs: false,
        }),
      ],
      controllers: [OrdersController],
    })
    class ObjectivesModule {}

    app = await NestFactory.create(ObjectivesModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    app.setGlobalPrefix("api");
    app.enableVersioning({ type: VersioningType.URI });
    await app.listen(0);
    baseUrl = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
    await new Promise((resolve) => collector.close(resolve));
  });

  it("sends a handler's objectives with the route it actually served", async () => {
    const created = await fetch(`${baseUrl}/api/v1/orders`, { method: "POST" });
    expect(created.status).toBe(201);

    await waitFor(
      () => declarations().length > 0,
      8_000,
      "the declaration to arrive",
    );

    expect(declarations()).toEqual([
      {
        handler: "OrdersController.create",
        operationId: "/api/v1/orders",
        method: "POST",
        objectives: [
          { availability: 99.9, latency: { underMs: 300, target: 99 } },
        ],
      },
    ]);
  });

  it("states a route once per process, and nothing for handlers without objectives", async () => {
    await fetch(`${baseUrl}/api/v1/orders`, { method: "POST" });
    await fetch(`${baseUrl}/api/v1/orders`);
    // Two flush intervals, so anything these requests would have declared
    // has had the chance to arrive.
    await new Promise((resolve) => setTimeout(resolve, 2_500));

    expect(declarations()).toHaveLength(1);
  });
});
