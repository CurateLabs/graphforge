# GeoArrow producer conformance

GraphForge publishes consumer-neutral GeoArrow fixtures under
`tests/fixtures/geoarrow-v1/`. `canonical.arrow` is Arrow IPC stream data and
`canonical.parquet` is the equivalent Parquet data. Their source contract is
`tests/contracts/geoarrow-interchange-v1.json`; regenerate both files with:

```bash
cargo run -p graphforge-cli --example generate_geoarrow_fixtures
```

The catalog covers all six initial geometry layouts, both computationally
certified CRS profiles, a preserved-only extension/CRS/edge metadata profile,
one populated and one null row, and every resulting nested offset buffer. The
Python binding acceptance test reads the checked-in IPC and Parquet files
directly with PyArrow and compares extension names, metadata, coordinates,
nesting, batch sizes, and nulls to the JSON contract. No GraphForge field
renaming or WKB, WKT, or GeoJSON reconstruction participates in that check.

## Compatibility and failures

Certified EPSG:4326 and EPSG:3857 values receive GraphForge validation and the
documented coordinate, geometry, vertex, and byte limits. Preserved-only
profiles must use a recognized physical geometry layout and explicitly carry a
non-empty extension name plus JSON extension metadata containing a CRS. The
same geometry and byte limits apply. Malformed envelopes, incompatible storage
types, and exceeded limits fail before publication with stable validation
domains; diagnostics never include coordinate values.

GraphForge does not reproject preserved-only values. `point()` accepts only the
certified construction profiles, while `distance()` requires compatible
certified Point values. Unsupported computation is an explicit error and does
not alter the stored value.

## Consumer boundary

Any standards-aware Arrow consumer may use these files as compatibility
fixtures. Consumers own their parsing integration, projection, rendering,
interaction, and release tests. GraphForge neither imports a visualization
runtime nor requires XYG, a map, scene, tile service, or renderer to pass its
producer close gate.
