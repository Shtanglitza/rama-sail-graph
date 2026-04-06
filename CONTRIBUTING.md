# Contributing to rama-sail-graph

Thank you for your interest in contributing! This document provides guidelines and information for contributors.

## Prerequisites

- Java 17+
- [Leiningen](https://leiningen.org/)
- [Rama 1.6.0](https://redplanetlabs.com/docs) — required for integration tests. Rama includes a free embedded license for clusters up to two Supervisor nodes.

## Development Workflow

### Quick Start

```bash
# Check compilation (~30s)
lein check

# Run unit tests (no Rama cluster needed, ~5s)
lein test :only rama-sail.sail.optimizer-test rama-sail.sail.adapter-test

# Run full test suite (starts in-process cluster, ~2-5min)
lein test
```

### REPL-Based Development (Recommended)

For the fastest feedback loop:

```bash
# Start REPL
lein repl

# In another terminal, reload and test
clj-nrepl-eval -p <port> "(require 'rama-sail.sail.optimization :reload)"
clj-nrepl-eval -p <port> "(require 'rama-sail.sail.optimizer-test :reload) (clojure.test/run-tests 'rama-sail.sail.optimizer-test)"
```

### Test Categories

| Category | IPC Required | Time | Command |
|----------|-------------|------|---------|
| Unit tests | No | ~5s | `lein test :only rama-sail.sail.optimizer-test rama-sail.sail.adapter-test` |
| Default tests | Yes | ~2-5min | `lein test` |
| Benchmarks | Yes | ~10+min | `lein test :bench` |

## Code Style

- Follow idiomatic Clojure conventions
- Match the patterns already established in the codebase
- Use `defn-` for private functions
- Add docstrings to all public functions
- Prefer pure functions; isolate side effects at boundaries

## API Stability Tiers

When making changes, be aware of the stability tier of the namespace you're modifying:

- **Stable** (`rama-sail.core`, `rama-sail.sail.adapter`, `rama-sail.server.*`): Breaking changes require discussion and a major version bump.
- **Extension** (`rama-sail.module.*`, `rama-sail.metrics*`): Changes should be noted in CHANGELOG.
- **Internal** (`rama-sail.query.*`, `rama-sail.sail.compilation/optimization/serialization`, `rama-sail.errors`): Free to refactor.

## Pull Request Process

1. Create a branch from `main`
2. Make your changes with tests
3. Ensure `lein test :only rama-sail.sail.optimizer-test rama-sail.sail.adapter-test` passes
4. Update `CHANGELOG.md` under `[Unreleased]`
5. Open a PR with a clear description of what and why

## Reporting Issues

Use [GitHub Issues](https://github.com/Shtanglitza/rama-sail-graph/issues) with the provided templates. Include:
- Environment details (Java version, OS, Rama version)
- Steps to reproduce
- SPARQL query if applicable
