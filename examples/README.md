# Example projects

These four portable repositories demonstrate the complete Project/v1 and
ExperimentCard/v1 surface without Flowers paths or scheduler plugins. Copy one
directory into its own Git repository, create host-local Enrollment bindings,
commit the files, then use `experiment-queue project register`, `project doctor`,
and `submit --dry-run` before admission.

- `ordinary`: one direct process and one required result artifact;
- `python-training`: a training-shaped job with logical data and artifact mounts;
- `data-pipeline`: a read-only input mount and a directory output;
- `cooperative-preemption`: a project-owned typed checkpoint adapter using the
  public `CooperativeYieldHelper`.

The queue injects logical mount paths as `EXPERIMENT_QUEUE_MOUNT_<NAME>` and
exact declared output paths as `EXPERIMENT_QUEUE_ARTIFACT_<NAME>`, with hyphens
converted to underscores. Host paths belong only in Enrollment documents.
