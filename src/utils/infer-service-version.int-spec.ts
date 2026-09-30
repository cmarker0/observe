import {
  Controller,
  DynamicModule,
  Get,
  INestApplication,
  Injectable,
  Logger,
  Module,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  ObserveOptions,
  ObserveOptionsFactory,
} from "../interfaces/observe-options.interface.js";
import { createObserveModule } from "../observe.module.js";
import {
  FakeCollector,
  ReceivedBatch,
  startFakeCollector,
} from "../testing/fake-collector.js";

/**
 * Every variable inference consults, commits and revisions alike. All are
 * cleared before each test: CI sets some of them (`GITHUB_SHA`,
 * `CI_COMMIT_SHA`) for the length of the job, and one left in place would
 * answer before the variable a test sets - or instead of the checkout it
 * builds.
 */
const RELEASE_VARIABLES = [
  "OBSERVE_SERVICE_VERSION",
  "VERCEL_GIT_COMMIT_SHA",
  "RENDER_GIT_COMMIT",
  "RAILWAY_GIT_COMMIT_SHA",
  "HEROKU_BUILD_COMMIT",
  "HEROKU_SLUG_COMMIT",
  "SOURCE_COMMIT",
  "GIT_REV",
  "GITHUB_SHA",
  "CI_COMMIT_SHA",
  "BITBUCKET_COMMIT",
  "CIRCLE_SHA1",
  "BUILDKITE_COMMIT",
  "BUILD_SOURCEVERSION",
  "CODEBUILD_RESOLVED_SOURCE_VERSION",
  "GIT_COMMIT_SHA",
  "GIT_COMMIT",
  "GIT_SHA",
  "COMMIT_SHA",
  "SOURCE_VERSION",
  "K_REVISION",
  "CONTAINER_APP_REVISION",
];

const PLATFORM_SHA = "5f1c3b8e9a2d4c6b7e0f1a2b3c4d5e6f7a8b9c0d";
const CHECKOUT_SHA = "9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d";

type ObserveModuleClass = ReturnType<
  typeof createObserveModule
>["ObserveModule"];

@Controller()
class PingController {
  @Get("ping")
  ping() {
    return { ok: true };
  }
}

/**
 * Runs `define` as though the process had been started from `directory`.
 * Inference searches the working directory and then the entry script's, and
 * under Vitest the entry script sits in this repository's node_modules -
 * inside this very checkout - so both point at `directory` for the length of
 * the call. `forRoot` infers synchronously, so nothing else ever sees either.
 */
function startedFrom<T>(directory: string, define: () => T): T {
  const cwd = process.cwd();
  const entry = process.argv[1];
  process.chdir(directory);
  process.argv[1] = join(directory, "main.js");
  try {
    return define();
  } finally {
    process.chdir(cwd);
    process.argv[1] = entry;
  }
}

/**
 * Release inference, end to end: an application that never names its
 * release still delivers one in the root of every batch the collector
 * receives.
 *
 * The unit specs pin how each source is parsed and ranked. Only a booted app
 * shows the answer is taken when the module is registered - by `forRoot`, or
 * by the `forRootAsync` factory - and survives the options provider, the
 * shared buffer, the worker's JSON and gzip to arrive as `serviceVersion`,
 * still clamped to what the collector accepts; and that `serviceVersion:
 * false` keeps it off the wire even here, where this repository's own
 * checkout is always within reach.
 */
describe("Release inference: the release a batch carries", () => {
  let collector: FakeCollector;
  let app: INestApplication | undefined;
  let scratch: string;
  // A monorepo app inside a checkout, and a directory in no checkout at all -
  // what a container image built without `.git` looks like.
  let checkoutApp: string;
  let image: string;
  let requests = 0;

  const options = (
    overrides: Partial<ObserveOptions> = {},
  ): ObserveOptions => ({
    appKey: "test-key",
    appSecret: "test-secret",
    serviceId: "release-app",
    endpoint: collector.url,
    flushInterval: 1000,
    runtimeMetrics: false,
    forwardLogs: false,
    ...overrides,
  });

  /** One request through `target`, and the batch that delivered it. */
  const send = async (target: INestApplication): Promise<ReceivedBatch> => {
    const traceId = `release-${++requests}`;
    const response = await fetch(`${await target.getUrl()}/ping`, {
      headers: { "x-request-id": traceId },
    });
    expect(response.status).toBe(200);
    return collector.waitForTrace(traceId);
  };

  /**
   * Registers the module the way `register` says - which, for `forRoot`, is
   * the moment the environment is read - boots an app around it, and returns
   * the batch that carried its first request.
   */
  const firstBatch = async (
    register: (observe: ObserveModuleClass) => DynamicModule,
  ): Promise<ReceivedBatch> => {
    const { ObserveModule, ObserveInstrument } = createObserveModule();

    @Module({
      imports: [register(ObserveModule)],
      controllers: [PingController],
    })
    class ReleaseAppModule {}

    app = await NestFactory.create(ReleaseAppModule, {
      instrument: ObserveInstrument,
      logger: false,
    });
    await app.listen(0);
    return send(app);
  };

  beforeAll(async () => {
    collector = await startFakeCollector();

    scratch = mkdtempSync(join(tmpdir(), "observe-release-int-"));
    const gitDir = join(scratch, "checkout", ".git");
    mkdirSync(join(gitDir, "refs", "heads"), { recursive: true });
    writeFileSync(join(gitDir, "HEAD"), "ref: refs/heads/main\n");
    writeFileSync(join(gitDir, "refs", "heads", "main"), `${CHECKOUT_SHA}\n`);
    checkoutApp = join(dirname(gitDir), "apps", "api");
    mkdirSync(checkoutApp, { recursive: true });
    image = join(scratch, "image");
    mkdirSync(image);
  });

  afterAll(async () => {
    await collector?.close();
    rmSync(scratch, { recursive: true, force: true });
  });

  beforeEach(() => {
    for (const name of RELEASE_VARIABLES) {
      vi.stubEnv(name, undefined);
    }
  });

  afterEach(async () => {
    await app?.close();
    app = undefined;
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("sends the release the application names, whatever the environment says", async () => {
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "from-the-environment");
    vi.stubEnv("GITHUB_SHA", PLATFORM_SHA);

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options({ serviceVersion: "4.2.0" })),
    );

    expect(batch.serviceVersion).toBe("4.2.0");
  });

  it("cuts a release the application names to the 50 characters the collector accepts, and says so", async () => {
    // Sent whole, it would cost every batch: the collector refuses a batch
    // whose version is too long rather than cutting it.
    const warn = vi
      .spyOn(Logger.prototype, "warn")
      .mockImplementation(() => undefined);
    const named =
      "123456789012.dkr.ecr.eu-west-1.amazonaws.com/orders-api:2026.09.30-1";

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options({ serviceVersion: named })),
    );

    expect(batch.serviceVersion).toHaveLength(50);
    expect(batch.serviceVersion?.startsWith(named.slice(0, 41))).toBe(true);
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining(`is sent as "${batch.serviceVersion}"`),
    );
  });

  it("sends OBSERVE_SERVICE_VERSION when the application names none, ahead of a platform's commit", async () => {
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "2026.09.29-rc.1");
    vi.stubEnv("VERCEL_GIT_COMMIT_SHA", PLATFORM_SHA);

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options()),
    );

    expect(batch.serviceVersion).toBe("2026.09.29-rc.1");
  });

  it.each([
    // Two platforms: the order they are listed in decides.
    ["VERCEL_GIT_COMMIT_SHA", "RENDER_GIT_COMMIT"],
    // The platform running the process, over the CI job that built it.
    ["HEROKU_SLUG_COMMIT", "GITHUB_SHA"],
    // A CI provider's own name, over a conventional one a team might set.
    ["CI_COMMIT_SHA", "GIT_COMMIT"],
  ])("sends %s over %s when both are set", async (preferred, passedOver) => {
    vi.stubEnv(passedOver, "b".repeat(40));
    vi.stubEnv(preferred, "a".repeat(40));

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options()),
    );

    expect(batch.serviceVersion).toBe("a".repeat(40));
  });

  it("cuts a SHA-256 commit to the 50 characters the collector accepts, keeping its head", async () => {
    // A longer version is not truncated by the collector: the whole batch it
    // rides in is refused.
    const sha256 = "0123456789abcdef".repeat(4);
    vi.stubEnv("GITHUB_SHA", sha256);

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options()),
    );

    expect(batch.serviceVersion).toBe(sha256.slice(0, 50));
    expect(collector.violations).toEqual([]);
  });

  it("keeps the last 50 characters of a long Cloud Run revision, the part each deploy changes", async () => {
    const revision = `${"payments-gateway-".repeat(3)}00042-xyz`;
    vi.stubEnv("K_REVISION", revision);

    const batch = await firstBatch((ObserveModule) =>
      startedFrom(image, () => ObserveModule.forRoot(options())),
    );

    expect(revision.length).toBeGreaterThan(50);
    expect(batch.serviceVersion).toBe(revision.slice(-50));
    expect(batch.serviceVersion?.endsWith("-00042-xyz")).toBe(true);
  });

  it("falls back to the commit checked out where the process was started", async () => {
    const batch = await firstBatch((ObserveModule) =>
      startedFrom(checkoutApp, () => ObserveModule.forRoot(options())),
    );

    expect(batch.serviceVersion).toBe(CHECKOUT_SHA);
  });

  it("prefers that commit to a container platform's revision name", async () => {
    vi.stubEnv("CONTAINER_APP_REVISION", "orders-api--0000042");

    const batch = await firstBatch((ObserveModule) =>
      startedFrom(checkoutApp, () => ObserveModule.forRoot(options())),
    );

    expect(batch.serviceVersion).toBe(CHECKOUT_SHA);
  });

  it("sends no release when nothing names one", async () => {
    const batch = await firstBatch((ObserveModule) =>
      startedFrom(image, () => ObserveModule.forRoot(options())),
    );

    expect(batch).not.toHaveProperty("serviceVersion");
  });

  it("sends no release at all when told not to, whatever the environment says", async () => {
    // Started from this repository, too, whose own checkout would otherwise
    // answer.
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "from-the-environment");
    vi.stubEnv("GITHUB_SHA", PLATFORM_SHA);

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options({ serviceVersion: false })),
    );

    expect(batch).not.toHaveProperty("serviceVersion");
  });

  it("infers the release for an empty serviceVersion, the way an unset config value arrives", async () => {
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "2026.09.29-2");

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options({ serviceVersion: "" })),
    );

    expect(batch.serviceVersion).toBe("2026.09.29-2");
  });

  it("infers the release on the forRootAsync path, from the environment its factory runs in", async () => {
    vi.stubEnv("RENDER_GIT_COMMIT", PLATFORM_SHA);

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRootAsync({ useFactory: async () => options() }),
    );

    expect(batch.serviceVersion).toBe(PLATFORM_SHA);
  });

  it("infers the release through an options factory class as well", async () => {
    vi.stubEnv("RAILWAY_GIT_COMMIT_SHA", PLATFORM_SHA);

    @Injectable()
    class ObserveConfig implements ObserveOptionsFactory {
      createObserveOptions() {
        return options();
      }
    }

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRootAsync({ useClass: ObserveConfig }),
    );

    expect(batch.serviceVersion).toBe(PLATFORM_SHA);
  });

  it("keeps the release it booted with when the environment changes afterwards", async () => {
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "booted-release");
    const first = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options()),
    );

    vi.stubEnv("OBSERVE_SERVICE_VERSION", "later-release");
    const later = await send(app!);

    expect(collector.batches.indexOf(later)).toBeGreaterThan(
      collector.batches.indexOf(first),
    );
    expect(first.serviceVersion).toBe("booted-release");
    expect(later.serviceVersion).toBe("booted-release");
  });

  it("says where the release came from when debug is on", async () => {
    const debug = vi
      .spyOn(Logger.prototype, "debug")
      .mockImplementation(() => undefined);
    vi.stubEnv("OBSERVE_SERVICE_VERSION", "2026.09.29-3");

    const batch = await firstBatch((ObserveModule) =>
      ObserveModule.forRoot(options({ debug: true })),
    );

    expect(batch.serviceVersion).toBe("2026.09.29-3");
    expect(debug).toHaveBeenCalledWith(
      expect.stringContaining(
        '"2026.09.29-3" inferred from OBSERVE_SERVICE_VERSION',
      ),
    );
  });

  it("says so when debug is on and no release could be found", async () => {
    const debug = vi
      .spyOn(Logger.prototype, "debug")
      .mockImplementation(() => undefined);

    await firstBatch((ObserveModule) =>
      startedFrom(image, () => ObserveModule.forRoot(options({ debug: true }))),
    );

    expect(debug).toHaveBeenCalledWith(
      expect.stringContaining("none could be inferred"),
    );
  });
});
