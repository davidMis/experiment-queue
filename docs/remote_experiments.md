# Remote Experiment Workflow

This document describes the lightweight local-development to remote-GPU workflow
supported by `scripts/run_experiment.py`.

## Intended Loop

1. Develop locally in the repository-local `.venv`.
2. Keep reusable code in top-level packages and scripts as thin wrappers.
3. Commit and push the exact code you want to run.
4. SSH to the GPU machine, pull or fetch the repository, and check out the
   intended commit SHA.
5. Activate `.venv` on the GPU machine and install recorded dependencies from
   `pyproject.toml`.
6. Launch long-running work through the experiment runner.
7. Pull the output directory back to the local workstation with the printed
   `rsync` command.

## GPU Environment

Follow the active setup in `README.md`. On the Linux GPU machine, prefer:

```sh
uv python install 3.14
uv venv --python 3.14 .venv
source .venv/bin/activate
uv pip install --python .venv/bin/python --upgrade -e ".[cuda12,dev,viz]"
python -c "import jax; print(jax.devices())"
```

## Runner Smoke Check

From the repository root:

```sh
source .venv/bin/activate
python scripts/run_experiment.py --help
```

The runner records provenance for the command placed after `--`.

## Example Remote Command

Use `--require-clean` for expensive runs that must correspond exactly to a git
commit. Omit it only when intentionally accepting a dirty worktree; in that
case the runner records `git_status.txt` and diff patches in the run directory.

This small synthetic benchmark command is a useful pattern because it does not
need a dataset path and prints its report to stdout, which the runner captures
in `stdout.log`:

```sh
python scripts/run_experiment.py \
  --name flowers-tiny-benchmark \
  --require-clean \
  --remote mutton2 \
  --notes "Tiny synthetic Flowers benchmark runner smoke check." \
  -- python scripts/benchmark_flowers.py \
    --preset tiny \
    --warmup 0 \
    --iterations 1
```

For training, evaluation, or data generation, replace the command after `--`
with the exact project command and flags from `README.md`.

## Run Directory Contents

Each run creates a directory under `outputs/experiments/` with a name like:

```text
20260608_153000_flowers-tiny-benchmark_a1b2c3d4/
```

The directory contains:

- `manifest.json`: command, paths, timing, git metadata, host metadata, copied
  configs, logs, and optional sync command.
- `stdout.log` and `stderr.log`: streamed process output.
- `configs/`: copied config files or directories passed with `--config`.
- `git_status.txt`, `git_diff.patch`, and `git_diff_cached.patch` when the
  worktree is dirty.

The child process receives:

- `EXPERIMENT_RUN_ID`: the run directory name.
- `EXPERIMENT_OUTPUT_DIR`: the absolute run directory path.

Training and analysis code should write run-specific artifacts under
`EXPERIMENT_OUTPUT_DIR` when it supports an explicit output directory. If a
script does not support that environment variable, pass an explicit output path
and record it in `log.md`.

## Pulling Outputs Back

When `--remote mutton2` is supplied, the runner prints a command like:

```sh
rsync -avh --progress mutton2:/path/to/repo/outputs/experiments/<run_id>/ outputs/experiments/<run_id>/
```

Run that command from the local workstation. Prefer `rsync` over `scp` because
it resumes partial transfers and skips files that are already current.
