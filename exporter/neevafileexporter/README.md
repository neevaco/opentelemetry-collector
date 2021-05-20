# Neeva File Exporter

This is a file exporter for traces, which writes JSON-encoded gzipped files, and supports writing to S3. The specific JSON format was made to be backwards-compatible with other existing tooling. The file names that are written to are rotated based on time or (uncompressed) byte size.

Supported pipeline types: traces

## Getting Started

The following settings are required:

- `root_path` (no default): Root path of rotated files.

The following settings are optional:

- `rotate_period` (default 0): Rotate files on this period, if 0 never rotate.
- `rotate_bytes` (default 0): Rotate files at this (uncompressed) size in bytes, if 0 never rotate.
- `json_workers` (default 1): Number of workers to marshal json.
- `json_bytes` (default 2048): Number of bytes to pre-allocate per span json marshal.

Example:

```yaml
exporters:
  neevafile:
    root_path: s3://mybucket/mydir
    rotate_period: 10m
    rotate_bytes: 500000000
    json_workers: 8
    json_bytes: 2048
```
