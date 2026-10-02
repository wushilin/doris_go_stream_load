# Rust SDK Stream Load Compression Notes

Apache Doris Stream Load supports compressed request bodies through the `compress_type` header. The documented values are `gz`, `lzo`, `bz2`, `lz4`, `lzop`, and `deflate`. This is Doris's file compression setting, not generic HTTP `Content-Encoding` negotiation. Although current docs describe this option as CSV-only, real Stream Load probes against our server succeeded for all six codecs with both CSV and JSON. Verify this behavior against the target Doris version.

## Request behavior

1. Serialize the batch to its normal CSV bytes.
2. If compression is `none` (the default), send the bytes unchanged and omit `compress_type`.
3. Otherwise, compress the complete CSV body with the selected codec, set `compress_type` to that exact Doris value, and calculate `Content-Length` from the compressed bytes.
4. Keep the usual CSV Stream Load headers (`format: csv`, `columns`, label, separators, and `Expect: 100-continue`). Do not set `Content-Encoding` for this feature.
5. Allow the listed codecs for CSV and JSON; reject compression for other formats.

## Values and wire formats

| SDK value | Doris `compress_type` | Encoding |
|---|---|---|
| `none` | omitted | Plain CSV |
| `gz` | `gz` | gzip stream |
| `lzo` | `lzo` | LZOP container (Doris routes both `lzo` and `lzop` to its LZOP decompressor) |
| `bz2` | `bz2` | BZip2 stream |
| `lz4` | `lz4` | LZ4 frame |
| `lzop` | `lzop` | LZOP container |
| `deflate` | `deflate` | zlib-wrapped DEFLATE stream |

Use Rust crates for the codecs so the SDK does not shell out to system programs. Check each crate's output format against Doris's expected format, especially LZOP framing and zlib-wrapped DEFLATE. Doris currently maps both the `lzo` and `lzop` values to its LZOP decompressor.

## Tests

- Round-trip the CSV through every codec and confirm that decoding reproduces the original bytes.
- Verify `none` preserves the body and omits `compress_type`.
- Verify each enabled codec sets the matching `compress_type` and sets `Content-Length` to the compressed body length.
- Reject unsupported compression values and compression with formats other than CSV or JSON.
- When a Doris cluster is available, load a row with each codec into a temporary table and verify the rows are queryable.

Use the Go SDK implementation in this repository as a reference for header names and per-codec behavior.
