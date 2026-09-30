import {
  Objective,
  OBJECTIVES_METADATA,
  objectiveProblems,
  toDeclaredObjective,
  unknownObjectiveFields,
} from "./objective.decorator.js";

describe("@Objective", () => {
  it("keeps every declaration a handler carries, in the order written", () => {
    class OrdersController {
      @Objective({ availability: 99.9 })
      @Objective({ latency: { underMs: 300, target: 99 } })
      create() {}
    }

    const declared = Reflect.getMetadata(
      OBJECTIVES_METADATA,
      Object.getOwnPropertyDescriptor(OrdersController.prototype, "create")!
        .value,
    );
    expect(declared).toEqual([
      { availability: 99.9 },
      { latency: { underMs: 300, target: 99 } },
    ]);
  });
});

describe("what an objective is sent as", () => {
  // Options built outside an object literal - read from a shared constants
  // file, say - carry whatever else that object had, and TypeScript lets it.
  const loose = {
    availability: 99.9,
    owner: "payments-team",
    latency: { underMs: 300, target: 99, percentile: "p99" },
  };

  it("names the fields the collector does not declare", () => {
    expect(unknownObjectiveFields(loose)).toEqual([
      "owner",
      "latency.percentile",
    ]);
    expect(
      unknownObjectiveFields({ availability: 99.9, windowDays: 7, name: "x" }),
    ).toEqual([]);
  });

  it("keeps the declared fields and nothing else", () => {
    expect(toDeclaredObjective(loose)).toEqual({
      availability: 99.9,
      latency: { underMs: 300, target: 99 },
    });
    expect(
      toDeclaredObjective({
        latency: { underMs: 250, target: 95 },
        windowDays: 7,
        name: "Checkout",
      }),
    ).toEqual({
      latency: { underMs: 250, target: 95 },
      windowDays: 7,
      name: "Checkout",
    });
  });
});

describe("objectiveProblems", () => {
  it("accepts what the collector accepts", () => {
    expect(
      objectiveProblems({
        availability: 99.99,
        latency: { underMs: 250, target: 95 },
        windowDays: 7,
        name: "Checkout",
      }),
    ).toEqual([]);
  });

  it("says why a declaration would be refused", () => {
    expect(objectiveProblems({})).toEqual([
      "declares neither availability nor latency",
    ]);
    expect(objectiveProblems({ availability: 0.999 })[0]).toContain(
      "availability must be a percentage",
    );
    expect(
      objectiveProblems({ latency: { underMs: 12.5, target: 99 } })[0],
    ).toContain("whole milliseconds");
    expect(
      objectiveProblems({ availability: 99, windowDays: 90 as never })[0],
    ).toContain("windowDays must be one of");
  });
});
