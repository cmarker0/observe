import {
  fitServiceVersion,
  MAX_SERVICE_VERSION_LENGTH,
} from "./fit-service-version.util.js";

/** A release named after the image it deploys: only the tag changes. */
const IMAGE = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/orders-api";

/**
 * The collector refuses a whole batch whose version is over 50 characters, so
 * every version the SDK sends, named or inferred, is cut here first.
 */
describe("fitServiceVersion", () => {
  it("keeps a version that fits as it is", () => {
    const longest = "r".repeat(MAX_SERVICE_VERSION_LENGTH);

    expect(fitServiceVersion("2.4.0")).toBe("2.4.0");
    expect(fitServiceVersion(longest)).toBe(longest);
  });

  it("keeps the head of a long commit, which is what identifies it", () => {
    // A SHA-256 repository's commit id is 64 characters.
    const sha256 = "0123456789abcdef".repeat(4);

    expect(fitServiceVersion(sha256)).toBe(sha256.slice(0, 50));
    expect(fitServiceVersion(sha256.toUpperCase())).toBe(
      sha256.toUpperCase().slice(0, 50),
    );
  });

  it("ends anything else in a hash of the whole, after its first 41 characters", () => {
    const fitted = fitServiceVersion(`${IMAGE}:2026.09.30-1`);

    expect(fitted).toHaveLength(MAX_SERVICE_VERSION_LENGTH);
    // Pinned: every replica of a deploy has to send the same release, or one
    // release would arrive as several.
    expect(fitted).toBe("123456789012.dkr.ecr.eu-west-1.amazonaws.~d8c2b916");
  });

  it("keeps releases that differ only past the cut apart", () => {
    const first = fitServiceVersion(`${IMAGE}:2026.09.30-1`);
    const second = fitServiceVersion(`${IMAGE}:2026.09.30-2`);

    expect(first.slice(0, 41)).toBe(second.slice(0, 41));
    expect(first).not.toBe(second);
  });
});
