# Contributing to bubu-sdk-go

The Go SDK powers every Engram and Impulse in BubuStack. Thanks for helping us keep it reliable, ergonomic, and production-ready.

## Reporting bugs

- Check [existing issues](https://github.com/bubustack/bubu-sdk-go/issues?q=is%3Aissue) first.
- Include the following details when filing a bug:
  - Engram/Impulse name and execution mode (batch, streaming, impulse) using the SDK.
  - Minimal code sample or `Story` snippet that reproduces the issue.
  - SDK version (tag/commit), Kubernetes version, and controller versions you tested with.
  - Logs with `BUBU_DEBUG=true`, including stack traces or transport payloads if relevant.
- Apply the `kind/bug`, appropriate `area/*`, and `priority/*` labels when triaging.

## Requesting enhancements

- Use the [feature request template](https://github.com/bubustack/bubu-sdk-go/issues/new?template=feature_request.md).
- Describe the workflow or scale constraint that motivates the change, and outline the desired API (interfaces, config structs, helper functions).
- If the change spans other repos (operators, Engram templates), open an org-level discussion so we can coordinate releases.

## Pull requests

1. Fork the repo, branch from `main`, and keep the PR focused.
2. Follow existing package boundaries (`batch`, `stream`, `impulse`, `runtime`, etc.); avoid introducing new dependencies without discussion.
3. Run the quality gates before requesting review:
   ```bash
   make lint             # golangci-lint (downloaded to ./bin)
   make test             # fmt + vet + go test ./... -race
   make test-integration # optional envtest smoke tests (requires KUBEBUILDER_ASSETS)
   make test-coverage    # optional coverage profile
   make tidy             # keep go.mod/go.sum clean
   ```
4. Update docs/examples/README when you add new APIs or change behaviour. Mention whether bobrapet/operator updates are required.
5. Use the PR template to record commands executed, test evidence, and linked issues (e.g., `Fixes #123`).

## Development workflow

### Prerequisites

- Go 1.26+ (matching `go.mod`).
- `make`, bash, and optionally Docker (only required for building sample binaries/images).

### Setup

1. Fork the repository and clone your fork.
2. `cd bubu-sdk-go`
3. `make help` to explore targets grouped by category.
4. `make lint-config` verifies the golangci-lint configuration if needed.

### Running tests

```bash
make test             # fmt + vet + go test ./... -race
make test-integration # optional envtest-backed suite
```

### Commit style & Code of Conduct

- Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) so changelog automation works (e.g., `feat: add structured message helper`, `fix: guard nil transport envelope`).
- Participation in this project is governed by the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). Report unacceptable behaviour to [conduct@bubustack.com](mailto:conduct@bubustack.com) or via the org Discussions moderation channel.
