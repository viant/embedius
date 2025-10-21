# Changelog

## Unreleased

- Unify default chunk size across splitters to 4096 bytes:
  - The large-file path in splitter.Factory now uses the same configured default chunk size as the standard path (defaults to 4096 when not specified).
  - This ensures consistent fragment sizes regardless of file size and helps control prompt size during retrieval.
  - A unit test was added to assert the behavior for large files.

### Affected files
- indexer/fs/splitter/factory.go
- indexer/fs/splitter/factory_test.go (new)

