# Example projects

These four portable repositories demonstrate the advanced Project/v1 and
ExperimentCard/v1 surface without Flowers paths or scheduler plugins. They use
logical mounts, observed artifacts, or cooperative preemption and therefore
need explicit host-local Enrollment bindings.

For an ordinary trusted project, start with `experiment-queue project init`
instead. Its `volumes: []` scaffold registers automatically against the
project's checkout-local `.venv` without an Enrollment file.

- `ordinary`: one direct process and one required result artifact;
- `python-training`: a training-shaped job with logical data and artifact mounts;
- `data-pipeline`: a read-only input mount and a directory output;
- `cooperative-preemption`: a project-owned typed checkpoint adapter using the
  public `CooperativeYieldHelper`.

The queue injects logical mount paths as `EXPERIMENT_QUEUE_MOUNT_<NAME>` and
exact declared output paths as `EXPERIMENT_QUEUE_ARTIFACT_<NAME>`, with hyphens
converted to underscores. Host paths belong only in Enrollment documents.
