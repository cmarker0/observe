import { All, Controller, Get, Logger, Version } from "@nestjs/common";
import { DiscoveryService, MetadataScanner } from "@nestjs/core";
import { RequestSnapshot } from "../interfaces/request-snapshot.interface.js";
import { Objective } from "./objective.decorator.js";
import { ObjectivesRegistry } from "./objectives.registry.js";

class OrdersController {
  @Objective({ availability: 99.9 })
  create() {}

  // Refused by the collector's bounds, so dropped at boot.
  @Objective({ availability: 12 })
  findAll() {}

  findOne() {}
}

const registry = (...controllers: Array<abstract new () => unknown>) => {
  const built = new ObjectivesRegistry(
    {
      getControllers: () =>
        (controllers.length > 0 ? controllers : [OrdersController]).map(
          (metatype) => ({ metatype }),
        ),
    } as unknown as DiscoveryService,
    new MetadataScanner(),
  );
  built.onModuleInit();
  return built;
};

const snapshot = (
  traces: Array<Record<string, unknown>>,
  overrides: Partial<RequestSnapshot> = {},
) =>
  ({
    traceId: "t-1",
    protocol: "http",
    operationId: "/api/v1/orders",
    attributes: { method: "POST" },
    traces,
    ...overrides,
  }) as unknown as RequestSnapshot;

const node = (
  className: string,
  methodKey: string,
  children: Array<Record<string, unknown>> = [],
) => ({ className, methodKey, children });

describe("ObjectivesRegistry", () => {
  it("reads only valid declarations off the controllers at boot", () => {
    expect(registry().size).toBe(1);
  });

  it("pairs a declaring handler with the route it served", () => {
    expect(
      registry().declarationFor(snapshot([node("OrdersController", "create")])),
    ).toEqual({
      handler: "OrdersController.create",
      operationId: "/api/v1/orders",
      method: "POST",
      objectives: [{ availability: 99.9 }],
    });
  });

  it("finds the handler under a guard or an interceptor's span", () => {
    expect(
      registry().declarationFor(
        snapshot([
          node("AuthGuard", "canActivate"),
          node("LoggingInterceptor", "intercept", [
            node("OrdersController", "create"),
          ]),
        ]),
      )?.handler,
    ).toBe("OrdersController.create");
  });

  it("states a route once, then again only after an hour", () => {
    const built = registry();
    const traces = [node("OrdersController", "create")];
    const now = 1_000_000;

    expect(built.declarationFor(snapshot(traces), now)).toBeDefined();
    expect(built.declarationFor(snapshot(traces), now + 1000)).toBeUndefined();
    expect(
      built.declarationFor(snapshot(traces), now + 60 * 60 * 1000),
    ).toBeDefined();
  });

  it("says nothing for a handler that declares nothing", () => {
    expect(
      registry().declarationFor(
        snapshot([node("OrdersController", "findOne")]),
      ),
    ).toBeUndefined();
  });

  describe("the method a route is declared under", () => {
    class StatusController {
      @Get()
      @Objective({ availability: 99.9 })
      show() {}

      @All("proxy")
      @Objective({ availability: 99 })
      proxy() {}
    }

    it("is the one the handler is routed for, so a HEAD probe of a GET route is the GET route", () => {
      const built = registry(StatusController);
      const traces = [node("StatusController", "show")];

      expect(
        built.declarationFor(
          snapshot(traces, {
            operationId: "/status",
            attributes: { method: "HEAD" },
          }),
        ),
      ).toMatchObject({ handler: "StatusController.show", method: "GET" });
      // The GET that follows is the same route, already stated.
      expect(
        built.declarationFor(
          snapshot(traces, {
            operationId: "/status",
            attributes: { method: "GET" },
          }),
        ),
      ).toBeUndefined();
    });

    it("is the request's own for a handler routed for every method", () => {
      expect(
        registry(StatusController).declarationFor(
          snapshot([node("StatusController", "proxy")], {
            operationId: "/proxy",
            attributes: { method: "PUT" },
          }),
        ),
      ).toMatchObject({ handler: "StatusController.proxy", method: "PUT" });
    });
  });

  it("finds the handler however many middlewares and interceptors wrap it", () => {
    expect(
      registry().declarationFor(
        snapshot([
          node("CorrelationMiddleware", "use", [
            node("RequestLogMiddleware", "use", [
              node("TransactionInterceptor", "intercept", [
                node("AuditInterceptor", "intercept", [
                  node("OrdersController", "create", [
                    node("OrdersService", "create"),
                  ]),
                ]),
              ]),
            ]),
          ]),
        ]),
      )?.handler,
    ).toBe("OrdersController.create");
  });

  describe("the collector's bounds", () => {
    let warn: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      warn = vi.spyOn(Logger.prototype, "warn").mockImplementation(() => {});
    });

    afterEach(() => {
      warn.mockRestore();
    });

    it("sends only the fields an objective has, and says which it left out", () => {
      const options = { availability: 99.9, owner: "payments-team" };
      class LooseController {
        @Objective(options)
        checkout() {}
      }

      expect(
        registry(LooseController).declarationFor(
          snapshot([node("LooseController", "checkout")]),
        )?.objectives,
      ).toEqual([{ availability: 99.9 }]);
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining(
          "@Objective on LooseController.checkout: owner not sent",
        ),
      );
    });

    it("sends the first ten objectives of a handler that declares more", () => {
      class PromisingController {
        checkout() {}
      }
      const descriptor = Object.getOwnPropertyDescriptor(
        PromisingController.prototype,
        "checkout",
      )!;
      // Applied bottom-up, as stacked decorators are: the first written is
      // applied last.
      for (
        let target = 99.9;
        target >= 98.9;
        target = Math.round((target - 0.1) * 10) / 10
      ) {
        Objective({ availability: target })(
          PromisingController.prototype,
          "checkout",
          descriptor,
        );
      }

      const declaration = registry(PromisingController).declarationFor(
        snapshot([node("PromisingController", "checkout")]),
      );

      expect(declaration?.objectives).toHaveLength(10);
      expect(declaration?.objectives[0]).toEqual({ availability: 98.9 });
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("only the first 10 of 11 are sent"),
      );
    });

    it("declares nothing for a route longer than the collector takes, and says so once", () => {
      const built = registry();
      const long = snapshot([node("OrdersController", "create")], {
        operationId: `/${"a".repeat(255)}`,
      });

      expect(built.declarationFor(long, 0)).toBeUndefined();
      expect(built.declarationFor(long, 2 * 60 * 60 * 1000)).toBeUndefined();
      // Once, not at every restatement - the fixture's refused objective
      // is warned about at boot as well, so only these count.
      expect(
        warn.mock.calls.filter((call: unknown[]) =>
          String(call[0]).includes("longer than 255 characters"),
        ),
      ).toHaveLength(1);
    });

    it("ignores a handler that serves several routes, however it comes to", () => {
      @Controller()
      class CatalogController {
        @Get(["catalog", "products"])
        @Objective({ availability: 99 })
        browse() {}
      }
      @Controller({ path: "orders", version: ["1", "2"] })
      class VersionedController {
        @Get()
        @Objective({ availability: 99 })
        list() {}

        // Its own version overrides the controller's: one route.
        @Get(":id")
        @Version("2")
        @Objective({ availability: 99 })
        findOne() {}
      }

      const built = registry(CatalogController, VersionedController);

      expect(built.size).toBe(1);
      expect(
        built.declarationFor(
          snapshot([node("CatalogController", "browse")], {
            operationId: "/catalog",
          }),
        ),
      ).toBeUndefined();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining(
          "@Objective on CatalogController.browse ignored: it serves 2 routes",
        ),
      );
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining(
          "@Objective on VersionedController.list ignored: it serves 2 routes",
        ),
      );
    });

    it("ignores a handler whose name is longer than the collector takes", () => {
      const methodName = `handle${"X".repeat(250)}`;
      class VerboseController {}
      Object.defineProperty(VerboseController.prototype, methodName, {
        value: function handler() {},
        writable: true,
        configurable: true,
      });
      Objective({ availability: 99.9 })(
        VerboseController.prototype,
        methodName,
        Object.getOwnPropertyDescriptor(
          VerboseController.prototype,
          methodName,
        )!,
      );

      expect(registry(VerboseController).size).toBe(0);
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("a handler name is at most 255 characters"),
      );
    });
  });
});
