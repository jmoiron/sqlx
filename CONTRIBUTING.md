# Contributing to sqlx

Thank you for your interest in contributing to sqlx.

## Before you open an issue or PR

- Search [existing issues](https://github.com/jmoiron/sqlx/issues) and [pull requests](https://github.com/jmoiron/sqlx/pulls) first.
- For bugs, include a minimal Go program that reproduces the problem, your Go version, and database driver if relevant.
- sqlx extends `database/sql`; confirm the behavior with plain `database/sql` when possible to isolate driver-specific issues.

## Pull requests

- Keep changes focused; avoid unrelated formatting or refactors.
- Add or update tests for behavior changes (`go test ./...`).
- Match existing code style in the files you touch.

## Development

```bash
go test ./...
```

Some tests require database drivers; run only package tests if you do not have databases configured locally.

## License

By contributing, you agree that your contributions are licensed under the same terms as the project (MIT).
