// Values that no honest caller sends: proxies and getters that throw, cycles,
// extreme depth, and numbers JSON cannot carry.

const throwing = () => {
  throw new Error("hostile");
};

/** A proxy whose every trap throws. */
export const everyTrapThrows = () =>
  new Proxy(
    {},
    new Proxy({}, { get: (_target, trap) => () => throwing(trap) }),
  );

export function hostileValues() {
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  const circular = {};
  circular.self = circular;
  let deep = {};
  for (let i = 0; i < 100_000; i += 1) deep = { a: deep };
  const bytesProxy = new Proxy(new Uint8Array(4), everyTrapThrows());
  return [
    everyTrapThrows(),
    new Proxy({}, { getPrototypeOf: throwing }),
    new Proxy({}, { ownKeys: throwing }),
    new Proxy({}, { get: throwing }),
    new Proxy([], { get: throwing }),
    new Proxy(() => 1, everyTrapThrows()),
    revoked.proxy,
    bytesProxy,
    {
      get member() {
        return throwing();
      },
    },
    { toJSON: throwing },
    {
      toJSON: () => ({
        get a() {
          return throwing();
        },
      }),
    },
    { [Symbol.toStringTag]: "Uint8Array", length: 4 },
    circular,
    deep,
    { value: 1n, again: [2n, { n: -3n }] },
    { n: Number.NaN },
    { n: Number.POSITIVE_INFINITY },
    { n: 2 ** 60 },
    new Date(0),
    new Map([["a", 1]]),
    Symbol("s"),
    () => 1,
    undefined,
    null,
    "\ud800",
    { "\udc00": 1 },
  ];
}
