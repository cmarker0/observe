import { Controller, Get, INestApplication, Module } from "@nestjs/common";
import { createServer, Server } from "node:http";
import { NestFactory } from "@nestjs/core";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createObserveModule } from "../observe.module.js";
import { CapturedOutput, captureOutput } from "../testing/observe-harness.js";

const { ObserveModule, ObserveInstrument } = createObserveModule();

// Fixed port: the decorator below is evaluated at import time, so the URL has
// to exist before any hook runs.
const COLLECTOR_PORT = 34517;
const COLLECTOR_URL = `http://127.0.0.1:${COLLECTOR_PORT}`;

@Controller()
class PingController {
  @Get("ping")
  ping() {
    return { ok: true };
  }
}

let collector: Server;
let rejected = 0;
let output: CapturedOutput;

@Module({
  imports: [
    ObserveModule.forRoot({
      // A revoked or mistyped pair. Not the `nest new` placeholders: those are
      // never sent at all (see scaffold-credentials.int-spec.ts).
      appKey: "revoked-app-key",
      appSecret: "revoked-app-secret",
      serviceId: "unauthenticated-app",
      endpoint: COLLECTOR_URL,
      // The agent's floor; anything lower is clamped to this with a warning.
      // The 2s waits below expect at least one flush at this cadence.
      flushInterval: 1000,
      runtimeMetrics: false,
      forwardLogs: false,
    }),
  ],
  controllers: [PingController],
})
class AppModule {}

let app: INestApplication;
let baseUrl: string;

beforeAll(async () => {
  collector = createServer((req, res) => {
    rejected += 1;
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ message: "Unauthorized" }));
  });
  await new Promise<void>((resolve) =>
    collector.listen(COLLECTOR_PORT, resolve),
  );

  output = captureOutput();

  app = await NestFactory.create(AppModule, {
    instrument: ObserveInstrument,
  });
  await app.listen(0);
  baseUrl = await app.getUrl();
});

afterAll(async () => {
  await app?.close();
  output?.restore();
  await new Promise<void>((resolve) => collector.close(() => resolve()));
});

const closeApp = async () => {
  await app.close();
  app = undefined as unknown as INestApplication;
};

it("keeps serving traffic while the collector rejects every batch", async () => {
  for (let i = 0; i < 5; i++) {
    const res = await fetch(`${baseUrl}/ping`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  }

  // Let the worker flush and be turned away.
  await new Promise((resolve) => setTimeout(resolve, 2_000));
  expect(rejected).toBeGreaterThan(0);

  // The point of the test: the app is still up and still correct afterwards.
  for (let i = 0; i < 5; i++) {
    const res = await fetch(`${baseUrl}/ping`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  }

  // Give the traffic above its own flush, so the collector turns the agent
  // away a second time.
  await new Promise((resolve) => setTimeout(resolve, 2_000));
  expect(rejected).toBeGreaterThan(1);

  // Said once, and it names the thing to fix - not one line per dropped batch.
  const complaints = output.lines.filter((line) =>
    line.includes("Telemetry rejected"),
  );
  expect(complaints).toHaveLength(1);
  expect(complaints[0]).toContain("appKey");

  // A deliberate shutdown is not a crash: `terminate()` reports exit code 1,
  // and treating that as a failure used to log an error and respawn a worker
  // thread during shutdown that nothing would ever stop.
  await closeApp();
  const restarts = output.lines.filter((line) =>
    line.includes("Restarting worker"),
  );
  expect(restarts).toHaveLength(0);
});
