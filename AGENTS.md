# Repository Instructions

These instructions apply to `experiment-queue`.

- Read `README.md`, `docs/architecture.md`, and relevant ADRs before changing
  public behavior or persistent state.
- Use the repository-local `.venv` with Python 3.14 or newer.
- Keep the queue service dependency-light and independent of every scientific
  project's environment. Never import project code into the service process.
- Treat database, manifest, receipt, and cooperative-yield versions as separate
  protocols. Do not reuse one version number for another.
- Preserve legacy behavior behind explicit compatibility boundaries; do not
  broaden Markdown parsing heuristics.
- Use temporary Git repositories and state directories in tests. Never point
  tests at operator state or launch against a live GPU allowlist.
- Scale verification with persistent-state, process-control, path-security, and
  web-authorization risk.
- Production support is Linux/NVIDIA. macOS checks are development-only.
- Do not connect to or operate a scientific project's remote host as part of
  queue development. Package reproducible operator commands instead.
