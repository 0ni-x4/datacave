## Contributing to Datacave

Thanks for your interest in contributing. This project values clear scope, high signal reviews, and incremental improvements.

## Code of Conduct

Be respectful and constructive. Harassment or abuse will not be tolerated.

## What to Work On

- Check open issues for planned work and priorities.
- If you want to propose a change, open an issue first to align on scope.
- Keep PRs focused and avoid unrelated refactors.

## Development Setup

```
cargo build --workspace
cargo test --workspace
```

Optional checks:

```
cargo fmt --all
cargo clippy --workspace --all-targets
```

## Pull Request Guidelines

- Keep PRs small and focused.
- Add tests for new behavior or bug fixes.
- Update docs when changing public behavior or config.
- Follow existing crate boundaries and module patterns.
- Avoid introducing new dependencies unless necessary.

## Commit Style

- Write concise, descriptive commit messages.
- Reference issues where applicable (e.g., "Fix #123").

## Reporting Security Issues

If you discover a security issue, please do not open a public issue. Email the project maintainer with details and a suggested mitigation.
