/** Convert a FixedSizeBinary UUID value into a stable comparison string. */
export const uuidHex = (value) => Buffer.from(value).toString("hex");

/** Convert one Arrow path-list row into its UUID comparison strings. */
export const pathHex = (table, row) =>
  Array.from(table.getChild("path").get(row), uuidHex);
