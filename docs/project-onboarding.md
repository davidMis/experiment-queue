# Project onboarding target

First-class project commands described here are target behavior and are not
implemented in the extracted baseline yet. Current implementation state lives
in `../llm/status.md`; task dependencies are in `../llm/todo.md`.

A new scientific project should ultimately need only:

1. `experiment-queue project init` to scaffold a tracked portable manifest;
2. one versioned YAML experiment card containing a direct argv command or a
   committed wrapper;
3. `experiment-queue project register /checkout/path` for host-local mounts and
   artifact roots;
4. `experiment-queue project doctor PROJECT` to validate Git, paths,
   environment, outputs, and optional cooperative preemption;
5. `experiment-queue submit PROJECT/CARD:JOB --dry-run`, followed by explicit
   admission.

Cards will carry universally useful identity, scientific intent, documentation,
parameters, job definitions, resources, artifacts, provenance, and declared
preemption capability. Project-specific structure belongs under namespaced
extensions. Most projects will not need a Python plugin.

The implemented typed library contract, extension envelope, bounded Submission
bindings, and immutable evidence fields are documented in
[`authoring-and-admission.md`](authoring-and-admission.md).

The project environment remains independent of the queue service. Domain code
creates and validates its checkpoints; an optional dependency-light protocol
helper will handle yield requests, atomic receipts, hashes, and failures.
