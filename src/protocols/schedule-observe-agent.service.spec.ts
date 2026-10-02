import { createRequire } from "module";
import { ScheduleObserveAgentService } from "./schedule-observe-agent.service.js";

/**
 * The explorer moved from a deep path into the package entry point in
 * @nestjs/schedule 12.0.1. Both must keep working: the supported peer range
 * spans versions on either side of that release.
 */
describe("ScheduleObserveAgentService: locating the explorer", () => {
  const warn = vi.fn();

  // Required rather than imported: on versions before 12.0.1 the entry point
  // has no such export, and a static named import would not link.
  const entryPointExplorer = (
    createRequire(import.meta.url)("@nestjs/schedule") as {
      ScheduleExplorer?: unknown;
    }
  ).ScheduleExplorer;

  const loadScheduleExplorer = (load?: unknown) =>
    (
      ScheduleObserveAgentService.prototype as unknown as {
        loadScheduleExplorer: (
          load?: unknown,
        ) => { prototype?: Record<string, unknown> } | undefined;
      }
    ).loadScheduleExplorer.call({ logger: { warn } }, load);

  beforeEach(() => warn.mockClear());

  it("resolves the explorer the patch is applied to", () => {
    const explorer = loadScheduleExplorer();

    // The patch replaces this method, so resolving anything without it would
    // leave scheduled jobs silently uninstrumented.
    expect(explorer?.prototype?.wrapFunctionInTryCatchBlocks).toEqual(
      expect.any(Function),
    );
    expect(warn).not.toHaveBeenCalled();
  });

  it.skipIf(!entryPointExplorer)(
    "prefers the entry point export when the version has one",
    () => {
      expect(loadScheduleExplorer()).toBe(entryPointExplorer);
    },
  );

  it.skipIf(entryPointExplorer)(
    "falls back to the deep path on versions without the export",
    () => {
      const deepPath = createRequire(import.meta.url)(
        "@nestjs/schedule/dist/schedule.explorer.js",
      ) as { ScheduleExplorer?: unknown };

      expect(loadScheduleExplorer()).toBe(deepPath.ScheduleExplorer);
    },
  );

  describe("on a version whose entry point lacks the export", () => {
    // Stood in for, so these run whichever version is installed: the 12.0.1
    // devDependency exports the explorer, and the case above only runs on
    // versions before it.
    const explorer = { prototype: { wrapFunctionInTryCatchBlocks() {} } };
    const loadWith =
      (deepPath: unknown) =>
      (packageName: string, specifier: string = packageName) =>
        specifier === packageName ? { installed: true, module: {} } : deepPath;

    it("loads the explorer from the deep path", () => {
      const loaded = loadScheduleExplorer(
        loadWith({ installed: true, module: { ScheduleExplorer: explorer } }),
      );

      expect(loaded).toBe(explorer);
      expect(warn).not.toHaveBeenCalled();
    });

    it("warns, rather than failing, when the explorer is in neither place", () => {
      const loaded = loadScheduleExplorer(
        loadWith({ installed: true, module: {}, error: new Error("moved") }),
      );

      // Jobs that silently never appear would be the symptom otherwise.
      expect(loaded).toBeUndefined();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("scheduled jobs will not be instrumented"),
      );
    });
  });
});
