import { fitToLength } from "./fit-to-length.util.js";

/**
 * The collector refuses a whole batch over one value longer than it accepts,
 * so every bounded field is cut before it is sent - the same way, whichever
 * field it is.
 */
describe("fitToLength", () => {
  it("keeps a value that fits as it is", () => {
    expect(fitToLength("orders.placed", 100)).toBe("orders.placed");
    expect(fitToLength("x".repeat(100), 100)).toBe("x".repeat(100));
  });

  it("cuts a longer one to exactly the limit, keeping its head", () => {
    const value = `orders.${"placed.".repeat(30)}`;

    const fitted = fitToLength(value, 100);

    expect(fitted).toHaveLength(100);
    expect(value.startsWith(fitted.slice(0, 91))).toBe(true);
    expect(fitted).toMatch(/~[0-9a-f]{8}$/);
  });

  it("keeps values that differ only past the cut apart", () => {
    const head = "a".repeat(120);

    // Cut to their heads alone, these would arrive as one metric, one span
    // name, one service.
    expect(fitToLength(`${head}-one`, 100)).not.toBe(
      fitToLength(`${head}-two`, 100),
    );
  });

  it("cuts the same value the same way every time", () => {
    const value = "b".repeat(300);

    expect(fitToLength(value, 255)).toBe(fitToLength(value, 255));
  });

  it("does not split a character made of two code units", () => {
    // An emoji is two UTF-16 code units; cut between them, the wire would
    // carry half of it.
    const value = `${"c".repeat(90)}🙂${"d".repeat(40)}`;

    const fitted = fitToLength(value, 100);

    expect(fitted.length).toBeLessThanOrEqual(100);
    expect(fitted).not.toMatch(/[\ud800-\udbff](?![\udc00-\udfff])/);
  });
});
