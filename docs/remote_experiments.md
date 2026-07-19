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

## Invariant Flowers Gate 3 Benchmark

After committing the locally verified invariant model and updating the clean
remote checkout, the first GPU characterization is the production-width
`96^3` direct-decoder reference case:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 .venv/bin/python scripts/run_experiment.py \
  --name invariant-flowers-phase3-base96-direct \
  --require-clean \
  --remote mutton2 \
  --notes "Gate 3 production-width 96^3 direct-decoder compile, timing, and memory baseline." \
  -- .venv/bin/python scripts/benchmark_invariant_flowers.py \
    --preset base \
    --volume-shape 96 96 96 \
    --output-shape 96 96 \
    --core-grid-sizes 48 24 12 \
    --native-width 160 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --decoder surface_moment \
    --batch-size 1 \
    --frequency-count 76 \
    --frequency-max-hz 15 \
    --benchmarks forward train \
    --warmup 1 \
    --iterations 3 \
    --seed 0
```

The child writes `invariant_benchmark.json` into the runner-owned experiment
directory. The runner also retains `stdout.log`, the exact command, git/host
metadata, device-visible output, and the `rsync` pull command. Inspect this
first result before launching larger native grids: the production-width full
native path is intentionally expected to become memory-limited, and Gate 3
selects the next light-width/rematerialized case from measured headroom rather
than guessing a safe `512^3` configuration.

## Nested Progress Bars

The runner uses PTY mode by default so child commands can render TTY-aware
progress output, such as a top-level `tqdm` bar plus per-worker bars:

```sh
python scripts/run_experiment.py \
  --name paired-shard-smoke \
  --remote mutton2 \
  -- python scripts/run_paired_resolution_shards_parallel.py \
    --dataset-prefix pub_s05_pair_smoke \
    --num-shards 2 \
    --samples-per-shard 1
```

PTY mode makes the child process see a real terminal, so progress libraries can
render nested bars while the runner still records provenance. In this mode the
child command's stdout and stderr are merged into `stdout.log`, and
`stderr.log` contains a short note about the merge. Use `--no-pty` when plain
line-oriented logs or separate stdout/stderr files are preferable.

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
