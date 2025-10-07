# Contributing to the Bubu SDK for Go

First off, thank you for considering contributing. Your help is appreciated.

This document provides guidelines for contributing to the SDK. Please read it carefully to ensure a smooth collaboration process.

## How Can I Contribute?

### Reporting Bugs

- **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/bubustack/bubu-sdk-go/issues).
- If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/bubustack/bubu-sdk-go/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

### Suggesting Enhancements

- Open a new issue to discuss your enhancement. Clearly describe the proposed enhancement and its benefits.
- Provide code snippets, mockups, or diagrams to illustrate your idea.

### Pull Requests

- Fork the repository and create your branch from `main`.
- Ensure the test suite passes (`make test`).
- Make sure your code lints (`make lint`).
- Issue that pull request!

## Development Workflow

### Prerequisites

- Go 1.24+
- Docker
- `make`

### Setup

1.  Fork the repository.
2.  Clone your fork: `git clone https://github.com/your_username/bubu-sdk-go.git`
3.  Navigate to the repository directory: `cd bubu-sdk-go`
4.  Install dependencies: `go mod tidy`

### Running Tests

**Note:** `make test` is currently failing due to a known dependency issue. See the [troubleshooting guide](./docs/troubleshooting.md#go-vet-error-in-streamgo) for details.

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run linters and other checks
make all
```

### Commit Message Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification. This allows for automated changelog generation and semantic versioning.

Examples:
- `feat: Add support for custom retry policies`
- `fix: Correctly handle nil inputs in Process`
- `docs: Update README with new quickstart`
- `chore: Upgrade to Go 1.24`

## Documentation Style Guide

- **Structure**: All documentation pages should follow a consistent structure:
    - Brief introduction.
    - "When to use this" section for guides.
    - Concise, runnable examples.
    - Evidence citations for key claims.
    - Troubleshooting tips.
- **Headings**: Use sentence-case for H2/H3 headings (`## Like this`).
- **Code Fences**: Use language tags for all code blocks (e.g., `go`, `sh`, `yaml`).
- **Mermaid Diagrams**: Use Mermaid for diagrams and flowcharts.
- **`NEEDS-EVIDENCE`**: If a claim cannot be proven with a code citation, mark it with `NEEDS-EVIDENCE (path/to/file:line)` and a short plan for resolution.

### Proactive Agent Mode

When acting as the AI documentation owner, you should:
- Proactively make well-scoped improvements without asking for permission.
- Maintain a precise mental model of the SDK's components.
- Cross-check claims against the operator CRDs and gRPC proto definitions.

### Docusaurus Migration Notes

- Documentation in the `docs/` directory is intended for migration to a Docusaurus site.
- Use relative links for navigation.
- Frontmatter (id, title, slug) will be added in the external docs repository.

