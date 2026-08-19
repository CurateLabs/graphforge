# Canonical spatial values

GraphForge spatial properties use the GeoArrow extension-type contract. The
Rust engine owns validation, storage, query semantics, and Arrow publication;
renderers own projection, viewports, map data, geographic LOD, painting, and
interaction.

## Spatial v1 contract

Spatial property types are homogeneous and two-dimensional. An ontology must
declare both the geometry kind and CRS:

```yaml
properties:
  - owner: Place
    name: location
    type:
      spatial:
        geometry: point
        crs: "EPSG:4326"
    nullable: false
```

The geometry catalog is `point`, `linestring`, `polygon`, `multipoint`,
`multilinestring`, and `multipolygon`. The computationally certified CRSs are
EPSG:4326 and EPSG:3857. Axis order is always x/y: longitude/latitude for EPSG:4326 and
easting/northing for EPSG:3857. GraphForge never infers a CRS, swaps axes, or
silently converts coordinates.

Standards-valid fields using one of these physical geometry layouts may carry
another CRS or extension name as a **preserved-only profile**. Such values must
provide the exact extension name and metadata envelope. GraphForge retains that
metadata, coordinates, offsets, nesting, and nulls through Arrow ingestion,
Parquet storage, reopen, queries, IPC, and export. It does not claim those
profiles are computationally equivalent to a certified CRS: `point()` rejects
unsupported CRS construction and `distance()` rejects preserved-only values
instead of converting, inferring, or swapping axes.

Geometry collections, mixed-geometry columns, Z/M coordinates, arbitrary CRS
conversion, WKB, WKT, and GeoJSON are outside spatial v1.

## Arrow representation

Coordinates use GeoArrow's recommended separated representation: a non-null
`Struct<x: Float64 not null, y: Float64 not null>`. Geometry nesting uses Arrow
`List` arrays with the canonical child names:

| Geometry | Arrow storage |
| --- | --- |
| Point | coordinate struct |
| LineString | `List<vertices: coordinate>` |
| Polygon | `List<rings: List<vertices: coordinate>>` |
| MultiPoint | `List<points: coordinate>` |
| MultiLineString | `List<linestrings: List<vertices: coordinate>>` |
| MultiPolygon | `List<polygons: List<rings: List<vertices: coordinate>>>` |

Only the top-level property field carries `ARROW:extension:name`, set to the
corresponding `geoarrow.*` name, and `ARROW:extension:metadata`. CRS metadata is
canonical JSON using the GeoArrow `authority_code` form, for example:

```json
{"crs":"EPSG:4326","crs_type":"authority_code"}
```

Field names do not carry geometry semantics. Consumers must use the extension
metadata and must not infer longitude/latitude columns.

## Validation and privacy

Validation happens before publication. Arrays must match the canonical nested
type and metadata; child coordinates and geometry parts cannot be null;
coordinates must be finite and within the declared CRS bounds; polygon rings
must contain at least four vertices and close exactly. Geometry, vertex, and
byte budgets are bounded.

Stable errors identify only the validation category. Diagnostics may include
the geometry kind, CRS identifier, row/feature count, byte count, validation
phase, and error code. Coordinates and property values must never be logged.

The contract implementation is dependency-free beyond GraphForge's existing
Arrow stack. GraphForge does not depend on XYG, MapLibre, Leaflet, a tile
provider, or any visualization library.

## Delivery boundary

The Rust type, schema, metadata, and validation foundation is the first part of
issue #797. Persistence/query round trips, Python/Node/CLI exposure, and
cross-host fixtures are tracked as native children of that close-gate issue.
The published conformance catalog and independent-reader procedure are in
[GeoArrow producer conformance](geoarrow-conformance.md).
