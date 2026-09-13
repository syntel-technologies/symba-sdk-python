# First release notes — draft for maintainer review

These notes describe the consolidated code intended for the initial 0.1.0 release. They are **not an announcement that a release has shipped**. Use them to enrich the first Release Please PR; verify all claims against its final diff and CI before merging.

- Typed async and sync Python clients plus decorator-based workers and optional CLI/Redis integrations.
- Job chains, gates, retries, cooperative cancellation, checkpoints and event-driven resume.
- In-memory SymbaTest alongside conformance against a pinned real engine.
- Safer structured failure reporting, process/GPU executor hardening and worker capability advertisements.
- Python 3.11–3.14 checks, tested minimum dependencies, generated protocol drift detection and validated package distributions.

Canonical source: https://github.com/syntel-technologies/symba-sdk-python. Historical Amplior commits are preserved without rewriting their authors or messages. Ongoing development and release artifacts belong to Syntel.

Before publishing, include the exact engine/SDK compatibility reference, migration instructions, known limitations and final artifact links. The initial version is pre-1.0; do not advertise universal production readiness or performance beyond the measured environment.
