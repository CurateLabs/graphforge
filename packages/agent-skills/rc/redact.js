/** Redact volatile absolute paths and timestamps from RC evidence. */

const PATH_PATTERN = /(?:\/(?:private\/)?(?:var|tmp|Users|home)\/[^\s"'`]+|[A-Za-z]:\\[^\s"'`]+)/g;
const ISO_TIMESTAMP_PATTERN = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})/g;
const MICROS_KEY_PATTERN = /(timestamp|cutoff|valid_time).*micros$/i;

export function redactValue(value, pathHint = "") {
  if (typeof value === "string") {
    return value
      .replace(PATH_PATTERN, "<redacted-path>")
      .replace(ISO_TIMESTAMP_PATTERN, "<redacted-timestamp>");
  }
  if (typeof value === "number" && MICROS_KEY_PATTERN.test(pathHint)) {
    return "<redacted-micros>";
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => redactValue(item, `${pathHint}[${index}]`));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, redactValue(value[key], key)]),
    );
  }
  return value;
}

export function redactEvidence(evidence) {
  return redactValue(evidence);
}
