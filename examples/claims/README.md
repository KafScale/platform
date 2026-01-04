# Claims Registry

This directory is the **single source of truth** for all technical,
architectural, and product claims used across tutorials and examples.

## Rules

1. Every **non-trivial claim** must exist here exactly once.
2. Tutorials must **reference claims by ID**, not restate them freely.
3. Claims must include:
   - scope
   - status
   - evidence
   - limitations (if applicable)
4. Agents use this registry to prevent drift, overclaiming, and ambiguity.

## Claim Status

- **Draft** – proposed, not yet validated
- **Verified** – confirmed by docs, code, or experiments
- **Deprecated** – no longer true, kept for historical reference

## Naming Convention

KS-xxx-yyy

Examples:
- KS-ARCH-001 (architecture)
- KS-COMP-002 (compatibility)
- KS-LIMIT-001 (limitations)
- KS-PERF-001 (performance)

## Usage in tutorials

Example:

> KafScale uses stateless brokers  
> (see claim: **KS-ARCH-001**)