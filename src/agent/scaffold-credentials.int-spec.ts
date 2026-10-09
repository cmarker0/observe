import { Controller, Get, INestApplication, Module } from "@nestjs/common";
import { createServer, Server } from "node:http";
import { NestFactory } from "@nestjs/core";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createObserveModule } from "../observe.module.js";
import { CapturedOutput, captureOutput } from "../testing/observe-harness.js";

const { ObserveModule, ObserveInstrument } = createObserveModule();

// Fixed port: the decorator below is evaluated at import time, so the URL has
// to exist before any hook runs.
const COLLECTOR_PORT = 34521;
const COLLECTOR_URL = `http://127.0.0.1:${COLLECTOR_PORT}`;

@Controller()
class PingController {
  @Get("ping")
  ping() {
    return { ok: true };
  }
}

let collector: Server;
let received = 0;
let output: CapturedOutput;

@Module({
  imports: [
    ObserveModule.forRoot({
      // Exactly what `nest new --observe` scaffolds before the user pastes
      // their own credentials in.
      appKey: "YOUR_APP_KEY",
      appSecret: "YOUR_APP_SECRET",
      serviceId: "scaffolded-app",
      endpoint: COLLECTOR_URL,
      flushInterval: 1000,
      runtimeMetrics: true,
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
    received += 1;
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

it("points a freshly scaffolded app at the demo and sends nothing", async () => {
  for (let i = 0; i < 5; i++) {
    const res = await fetch(`${baseUrl}/ping`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  }

  // Two flushes' worth: a placeholder pair would have been turned away by now.
  await new Promise((resolve) => setTimeout(resolve, 2_500));
  expect(received).toBe(0);

  const notices = output.lines.filter((line) =>
    line.includes("not connected yet"),
  );
  expect(notices).toHaveLength(1);
  expect(notices[0]).toContain("https://www.observe-demo.nestjs.com/dashboard");

  // The first start of a new project must not greet anyone with an error.
  expect(output.lines.filter((line) => line.includes("Error:"))).toEqual([]);
  expect(
    output.lines.filter((line) => line.includes("Telemetry rejected")),
  ).toEqual([]);
});
