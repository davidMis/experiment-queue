# Flowers compatibility fixture

This is a local, non-production Project/v1 integration fixture. It does not
describe live queue contents and was created without inspecting or changing the
Flowers checkout or `mutton2`.

The three cards cover the compatibility shapes needed before cutover: a simple
job, a tracker-aware cooperative job, and an independently schedulable SPECFEM-
shaped worker. `make_enrollment.py` creates exact host-local Enrollment evidence
from operator-supplied paths; it never connects to a remote host.

Current/historical Markdown-card classification still requires the operator-
supplied offline inventory described in `docs/migrations/flowers-v4.md`.
