# Flowers compatibility fixture

This is a local, non-production Project/v1 integration fixture. It does not
describe live queue contents and was created without inspecting or changing the
Flowers checkout or `mutton2`.

Do not copy this advanced fixture as the production Flowers Project. It
deliberately exercises required mounts, artifact observation, extension
schemas, and cooperative preemption. Production Flowers should start with the
minimal `project init` scaffold (`volumes: []`) and automatic checkout-local
`.venv` enrollment.

The three cards cover the compatibility shapes needed before cutover: a simple
job, a tracker-aware cooperative job, and an independently schedulable SPECFEM-
shaped worker. `make_enrollment.py` creates exact host-local Enrollment evidence
from operator-supplied paths; it never connects to a remote host.

The selected production deployment starts fresh and does not classify or
import historical Markdown cards. New Flowers work must use committed
Project/v1 and ExperimentCard/v1 inputs.
