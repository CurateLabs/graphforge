/** Thin neighbourhood helper composing `forge.execute()`, mirroring Python recipes. */

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function identifier(name, kind) {
  if (typeof name !== "string" || !IDENTIFIER.test(name)) {
    throw new TypeError(
      `${kind} must be a valid identifier, got ${JSON.stringify(name)}`,
    );
  }
  return name;
}

function returnClause(canonicalProp) {
  if (canonicalProp === "name") {
    return "RETURN DISTINCT neighbour.name AS name, labels(neighbour) AS labels";
  }
  return (
    `RETURN DISTINCT neighbour.${canonicalProp} AS ${canonicalProp}, ` +
    "neighbour.name AS name, labels(neighbour) AS labels"
  );
}

/**
 * Return the n-hop neighbourhood of a seed node as an Arrow IPC Buffer.
 *
 * `hops === 0` returns a typed empty table with the same schema as a positive-hop
 * result (no traversal).
 */
export function neighbourhood(
  forge,
  canonical,
  hops = 2,
  { label = "Entity", canonicalProp = "canonical" } = {},
) {
  label = identifier(label, "label");
  canonicalProp = identifier(canonicalProp, "canonical_prop");
  if (typeof hops !== "number" || !Number.isInteger(hops) || hops < 0) {
    throw new TypeError(
      `hops must be an integer >= 0, got ${JSON.stringify(hops)}`,
    );
  }
  if (hops === 0) {
    return forge.execute(
      `MATCH (neighbour:${label}) WHERE false ${returnClause(canonicalProp)}`,
      { canonical },
    );
  }
  const query =
    `MATCH (seed:${label} {${canonicalProp}: $canonical})` +
    `-[*1..${hops}]-(neighbour:${label}) ` +
    `WHERE neighbour.${canonicalProp} <> $canonical ` +
    returnClause(canonicalProp);
  return forge.execute(query, { canonical });
}
