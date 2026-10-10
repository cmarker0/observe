// Request overhead of each way of tracing a Nest app, under load.
//
//   npm run build && node scripts/bench/otel-overhead.mjs [--seconds 10] [--connections 32]
//
// Every variant is a fresh child process serving the same endpoint - a
// controller over two providers - while this process drives keep-alive load
// at it and runs the sink every exporter ships to. The sink answers at once,
// so the cost of shipping is in the figures but no backend's is.
//
// Variants:
//   baseline          no tracing
//   registry          createObserveModule(): the snapshot agent and its worker
//   otel              createObserveModule({ opentelemetry }), BatchSpanProcessor
//   nestjs-core       @opentelemetry/instrumentation-nestjs-core, same SDK setup
//   http+otel         instrumentation-http beside the OTel recorder
//   http+nestjs-core  instrumentation-http with nestjs-core - the Kubernetes
//                     Operator's Node default
//
// Reported per variant: requests/s, latency p50/p99, and the server's CPU
// time per request (all threads - the registry's worker included).
import { fork } from "node:child_process";
import { createServer, Agent, request } from "node:http";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const VARIANTS = [
  "baseline",
  "registry",
  "otel",
  "nestjs-core",
  "http+otel",
  "http+nestjs-core",
];

if (process.argv.includes("--child")) {
  const { variant, sink } = JSON.parse(process.env.BENCH_CHILD);
  await serve(variant, sink);
} else {
  await drive();
}

async function drive() {
  const { values } = parseArgs({
    options: {
      seconds: { type: "string", default: "10" },
      warmup: { type: "string", default: "3" },
      connections: { type: "string", default: "32" },
      only: { type: "string" },
    },
  });
  const seconds = Number(values.seconds);
  const warmup = Number(values.warmup);
  const connections = Number(values.connections);
  const variants = values.only ? values.only.split(",") : VARIANTS;

  // What reached the sink while a variant was measured: bytes, and spans
  // for OTLP - proof each variant traced what it claims to.
  const shipped = { bytes: 0, spans: 0 };
  const sink = createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      const body = Buffer.concat(chunks);
      shipped.bytes += body.length;
      if (req.url === "/v1/traces") {
        for (const resource of JSON.parse(body).resourceSpans ?? []) {
          for (const scope of resource.scopeSpans ?? []) {
            shipped.spans += scope.spans?.length ?? 0;
          }
        }
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  await new Promise((resolve) => sink.listen(0, "127.0.0.1", resolve));
  const sinkUrl = `http://127.0.0.1:${sink.address().port}`;

  console.log(
    `node ${process.version}, ${connections} connections, ${warmup}s warm-up, ${seconds}s measured\n`,
  );
  console.log(
    "| variant | req/s | p50 ms | p99 ms | CPU µs/req | spans/req | shipped B/req |",
  );
  console.log("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
  for (const variant of variants) {
    const child = fork(fileURLToPath(import.meta.url), ["--child"], {
      env: { ...process.env, BENCH_CHILD: JSON.stringify({ variant, sink: sinkUrl }) },
      execArgv: variant === "baseline" || variant === "registry"
        ? []
        : ["--import", new URL("./register-hook.mjs", import.meta.url).pathname],
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    const ask = (message) =>
      new Promise((resolve) => {
        child.once("message", resolve);
        child.send(message);
      });
    const { port } = await new Promise((resolve) => child.once("message", resolve));
    const url = `http://127.0.0.1:${port}/orders/7`;

    await load(url, connections, warmup * 1000);
    const before = await ask("cpu");
    shipped.bytes = 0;
    shipped.spans = 0;
    const result = await load(url, connections, seconds * 1000);
    const after = await ask("cpu");
    // The registry's worker ships on its own timer.
    if (variant === "registry") {
      await new Promise((resolve) => setTimeout(resolve, 6000));
    }
    child.kill();

    const cpu = after - before;
    const sorted = result.latencies.sort((a, b) => a - b);
    const pick = (q) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    console.log(
      `| ${variant} | ${Math.round(result.count / seconds)} | ${pick(0.5).toFixed(2)} | ${pick(0.99).toFixed(2)} | ${Math.round(cpu / result.count)} | ${variant === "registry" || variant === "baseline" ? "-" : (shipped.spans / result.count).toFixed(1)} | ${Math.round(shipped.bytes / result.count)} |`,
    );
  }
  sink.close();
}

/** `connections` keep-alive loops hitting `url` until `ms` have passed. */
async function load(url, connections, ms) {
  const agent = new Agent({ keepAlive: true, maxSockets: connections });
  const latencies = [];
  const deadline = performance.now() + ms;
  const one = () =>
    new Promise((resolve, reject) => {
      const started = performance.now();
      request(url, { agent }, (res) => {
        res.resume();
        res.on("end", () => {
          if (res.statusCode !== 200) {
            reject(new Error(`status ${res.statusCode}`));
            return;
          }
          latencies.push(performance.now() - started);
          resolve();
        });
      })
        .on("error", reject)
        .end();
    });
  await Promise.all(
    Array.from({ length: connections }, async () => {
      while (performance.now() < deadline) {
        await one();
      }
    }),
  );
  agent.destroy();
  return { count: latencies.length, latencies };
}

async function serve(variant, sink) {
  const api = await import("@opentelemetry/api");
  const tracing = variant !== "baseline" && variant !== "registry";
  let provider;
  if (tracing) {
    const { AsyncLocalStorageContextManager } = await import(
      "@opentelemetry/context-async-hooks"
    );
    const { W3CTraceContextPropagator } = await import("@opentelemetry/core");
    const { BasicTracerProvider, BatchSpanProcessor } = await import(
      "@opentelemetry/sdk-trace-base"
    );
    const { OTLPTraceExporter } = await import(
      "@opentelemetry/exporter-trace-otlp-http"
    );
    api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    api.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
    provider = new BasicTracerProvider({
      spanProcessors: [
        new BatchSpanProcessor(new OTLPTraceExporter({ url: `${sink}/v1/traces` })),
      ],
    });
    api.trace.setGlobalTracerProvider(provider);

    const instrumentations = [];
    if (variant.startsWith("http+")) {
      const { HttpInstrumentation } = await import("@opentelemetry/instrumentation-http");
      instrumentations.push(new HttpInstrumentation());
    }
    if (variant.endsWith("nestjs-core")) {
      const { NestInstrumentation } = await import(
        "@opentelemetry/instrumentation-nestjs-core"
      );
      instrumentations.push(new NestInstrumentation());
    }
    const { registerInstrumentations } = await import("@opentelemetry/instrumentation");
    registerInstrumentations({ instrumentations });
  }

  // Imported only now, so the instrumentations above see them load.
  await import("reflect-metadata");
  const common = await import("@nestjs/common");
  const { NestFactory } = await import("@nestjs/core");
  const observe = await import("../../dist/index.js");

  class OrdersRepository {
    get(id) {
      return { id, total: 42 };
    }
  }
  common.Injectable()(OrdersRepository);

  class OrdersService {
    constructor(repository) {
      this.repository = repository;
    }
    async find(id) {
      await Promise.resolve();
      return this.repository.get(id);
    }
  }
  common.Injectable()(OrdersService);
  Reflect.defineMetadata("design:paramtypes", [OrdersRepository], OrdersService);

  class OrdersController {
    constructor(orders) {
      this.orders = orders;
    }
    findOne(id) {
      return this.orders.find(id);
    }
  }
  const findOne = Object.getOwnPropertyDescriptor(OrdersController.prototype, "findOne");
  common.Get("orders/:id")(OrdersController.prototype, "findOne", findOne);
  common.Param("id")(OrdersController.prototype, "findOne", 0);
  common.Controller()(OrdersController);
  Reflect.defineMetadata("design:paramtypes", [OrdersService], OrdersController);

  let observeModule = [];
  let instrument;
  if (variant === "registry" || variant.endsWith("otel")) {
    const { ObserveModule, ObserveInstrument } = observe.createObserveModule(
      variant === "registry" ? {} : { opentelemetry: true },
    );
    instrument = ObserveInstrument;
    observeModule = [
      ObserveModule.forRoot({
        appKey: "bench",
        appSecret: "bench",
        serviceId: "bench",
        endpoint: sink,
        runtimeMetrics: false,
        outgoing: false,
      }),
    ];
  }

  class AppModule {}
  common.Module({
    imports: observeModule,
    controllers: [OrdersController],
    providers: [OrdersService, OrdersRepository],
  })(AppModule);

  const app = await NestFactory.create(AppModule, {
    logger: false,
    ...(instrument && { instrument }),
  });
  await app.listen(0, "127.0.0.1");
  const { port } = app.getHttpServer().address();

  // Process-wide, so the registry's worker thread is counted too.
  process.on("message", async (message) => {
    if (message === "cpu") {
      await provider?.forceFlush();
      const { user, system } = process.cpuUsage();
      process.send(user + system);
    }
  });
  process.send({ port });
}
