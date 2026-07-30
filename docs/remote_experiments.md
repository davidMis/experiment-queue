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

## Mutton2 Manual-Execution Boundary

The user is the sole operator of `mutton2`. Codex prepares and verifies code
locally and provides copy-paste commands, but must not connect to, inspect,
launch on, monitor, or synchronize with that machine. The user runs each stage
below from the clean remote checkout and then runs the experiment runner's
printed `pull outputs with:` command from the local workstation. Stop after
every stage until its synced artifacts have been inspected.

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

## Cross-Flowers MVP Manual Run Queue

This queue supersedes the paused learned-native-shell benchmark sequence. The
historical first MVP endpoint-restricts the raw scalar wavespeed before the
learned lift, uses a fixed `48^3/24^3/12^3` core, retains `P = 64` cosine
modes, and evaluates all 76 bins on `0.0, 0.2, ..., 15.0 Hz`.
`cross_flowers` is the primary surface-query decoder and `surface_moment` is
its fixed-core control. There is no direct coefficient-query Cross decoder in
this implementation.

As of 2026-07-29, fresh models instead learn and output the 71 bins
`1.0, 1.2, ..., 15.0 Hz`; DC and `0.2/0.4/0.6/0.8 Hz` are excluded. The
Stage 1--9 commands are retained as provenance for completed runs and
compatible continuations of their historical checkpoints. Stage 10 is the
fresh 71-bin successor after the trainer, normalizer selection, checkpoint
metadata, and evaluator migration.

All commands below must be run manually by the user from `~/3D_Helmholtz` on
`mutton2`, at the intended committed SHA with a clean worktree. Each runner
copies `notes/2026_07_19_discretization_invariant_cross_flowers.tex` into the
run directory as the recorded architecture contract.

### Stage 1: Build the full resolution-balanced normalizer

Fit one train-only `(76, 2)` RMS scale. The explicit probabilities give equal
weight to resolution buckets, not to their unequal row counts. The script
streams one response at a time, which is the safe setting for `512^2` surfaces.
It is deterministic and therefore has no seed option.

```sh
cd ~/3D_Helmholtz
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-normalizer-76 \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Cross-Flowers train-only endpoint-quadrature RMS normalizer with equal 96/128/256/512 bucket weights." \
  -- .venv/bin/python scripts/build_resolution_transfer_normalizer.py \
    --manifest data/resolution_transfer/manifest.json \
    --data-dir data \
    --resolutions 96 128 256 512 \
    --bucket-probability 96=0.25 \
    --bucket-probability 128=0.25 \
    --bucket-probability 256=0.25 \
    --bucket-probability 512=0.25 \
    --expected-frequency-count 76 \
    --chunk-rows 1
```

The child writes
`pressure_normalizer_resolution_balanced_76.npz` and its provenance sidecar
`pressure_normalizer_resolution_balanced_76.json` at the top of the runner
directory. Run the printed `pull outputs with:` command from the local
workstation and verify the sidecar, SHA-256, bucket response counts, frequency
grid, scale range, and `production_shard_metadata_validated = true` before
continuing. Each bucket record must include a metadata SHA-256 for every train
shard. On `mutton2`, set the path printed by
the runner for use in later stages:

```sh
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/<normalizer-run-id>
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
test -f "$CROSS_FLOWERS_NORMALIZER"
```

Replace `<normalizer-run-id>` with the exact Stage 1 run-directory basename.
The normalizer remains an input artifact in its runner directory; do not copy
it into source-controlled model paths.

### Stage 2: GPU benchmark Cross and the matched control

First benchmark the primary Cross decoder at `96^3`. Run on an otherwise idle
GPU so allocator statistics are interpretable.

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-base96-benchmark \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Core-first Cross-Flowers 96^3 forward/train compile, timing, and memory gate." \
  -- .venv/bin/python scripts/benchmark_invariant_flowers.py \
    --preset base \
    --volume-shape 96 96 96 \
    --output-shape 96 96 \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --decoder cross_flowers \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --frequency-count 76 \
    --frequency-max-hz 15 \
    --benchmarks forward train \
    --warmup 1 \
    --iterations 3 \
    --seed 0
```

After syncing and inspecting its `invariant_benchmark.json`, run the matched
fixed-core moment control. The only intended model-path change is the decoder.

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-base96-surface-moment-benchmark \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Core-first 96^3 surface_moment control matched to the Cross-Flowers GPU benchmark." \
  -- .venv/bin/python scripts/benchmark_invariant_flowers.py \
    --preset base \
    --volume-shape 96 96 96 \
    --output-shape 96 96 \
    --core-grid-sizes 48 24 12 \
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

Sync each run with its own runner-generated pull instruction. Do not start
training unless both reports complete with
`numerical_checks.all_requested_operations_finite = true`, no OOM, and
acceptable measured GPU headroom for the primary Cross run. The report records
finite output leaves plus the training loss, gradient norm, metrics, updated
parameters, and optimizer state used by this gate.

### Stage 3: Matched four-row overfit gate

The allowed gate size is 1-8 rows; the first comparison uses four rows and 500
optimizer steps. This historical r096 gate used DC weight `0`,
`0.2-7.0 Hz` weight `1`, and `7.2-15.0 Hz` weight `0.1`.
The first matched attempt collapsed toward the zero-prediction loss after
Xavier output initialization produced losses of `46.94` (Cross) and `11.77`
(moment control). The rerun therefore uses a shared coefficient-output kernel
standard deviation of `1e-3`, chosen to match the roughly `1e-2` per-mode
target scale after summing 128 hidden inputs. Every evaluation traverses the
same four eligible rows one at a time, and `best.msgpack` is selected by their
aggregate loss rather than the most recently sampled training row.

Run the revised Cross model first:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-r096-overfit4-smallinit \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Cross-Flowers r096 four-row/500-step rerun with 1e-3 coefficient-output initialization and fixed aggregate evaluation." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train data/resolution_transfer/train/r096 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --overfit \
    --max-train-rows 4 \
    --steps 500 \
    --seed 0 \
    --log-every 10 \
    --eval-every 10 \
    --latest-every 100 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mvp-r096-overfit4-smallinit \
    --wandb-group cross-flowers-mvp-r096-overfit-smallinit \
    --wandb-tags cross-flowers mvp r096 overfit small-init
```

After syncing and inspecting it, run the same gate with the control:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-r096-surface-moment-overfit4-smallinit \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "surface_moment r096 four-row/500-step control rerun with 1e-3 coefficient-output initialization and fixed aggregate evaluation." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train data/resolution_transfer/train/r096 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder surface_moment \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --batch-size 1 \
    --overfit \
    --max-train-rows 4 \
    --steps 500 \
    --seed 0 \
    --log-every 10 \
    --eval-every 10 \
    --latest-every 100 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mvp-r096-surface-moment-overfit4-smallinit \
    --wandb-group cross-flowers-mvp-r096-overfit-smallinit \
    --wandb-tags surface-moment control mvp r096 overfit small-init
```

Each run writes `training/config.json`, `training/summary.json`, and
`training/checkpoints/{best,latest}.msgpack` under its runner directory. Sync
both runs. The gate passes only if losses and gradient norms remain finite,
the aggregate four-row Cross coefficient and reconstructed-surface errors
decrease substantially,
checkpoints can be read, and the Cross behavior is credible relative to the
matched control. Stop to diagnose architecture, normalization, or optimization
if those conditions fail.

### Stage 3b: Four-row constant-rate capacity diagnostic

The small-initialization Cross and moment runs reached aggregate losses
`0.972556` and `0.995335`, respectively. Cross learned about `5.9x` more by
improvement below the zero baseline, so the route-free control does not support
a Cross-specific optimization failure. Neither run memorized the four rows.
The original 500-step schedule spent 100 steps warming up and decayed to about
`1e-5` as the Cross curve began accelerating. Test that schedule hypothesis
before changing the objective or architecture: keep every model, data, loss,
seed, clipping, and optimizer setting fixed, but use a fresh 2,000-step
constant-`1e-4` run with no warmup. This remains a four-row capacity diagnostic,
not the full-data pilot in Stage 4. Evaluation records both the four-row mean
and each fixed row separately.

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-r096-overfit4-constant2k \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Cross-Flowers four-row capacity diagnostic with small initialization and a constant 1e-4 learning rate for 2,000 steps." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train data/resolution_transfer/train/r096 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --overfit \
    --max-train-rows 4 \
    --steps 2000 \
    --seed 0 \
    --learning-rate 0.0001 \
    --schedule constant \
    --warmup-steps 0 \
    --log-every 10 \
    --eval-every 10 \
    --latest-every 250 \
    --save-every 500 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mvp-r096-overfit4-constant2k \
    --wandb-group cross-flowers-mvp-r096-overfit-optimizer \
    --wandb-tags cross-flowers mvp r096 overfit constant-lr capacity
```

The completed run reached best aggregate loss `0.828887` at step 1980. It
learned strongly through `1 Hz`, partially through `3 Hz`, and not measurably
above `3 Hz`. This establishes a functioning capacity path without easy
full-band memorization. Constant `1e-4` also produced large clipped gradients
and an oscillatory curve, so it is not the long-run schedule.

### Stage 4: Overnight full-r096 training

This is the first substantial learning run after the finite GPU, initialization,
and capacity diagnostics. It uses the complete 9,000-row r096 training bucket
and a fixed 32-example r096 validation panel. It does not claim resolution
generalization; multi-resolution training and same-checkpoint transfer tests
follow only after its artifacts are reviewed. The measured Cross training time
is approximately `0.50 s/step`; 80,000 steps plus randomized data loading,
validation, and checkpointing should take roughly 11.5--12.5 hours.

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mvp-r096-long80k \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "First overnight Cross-Flowers full-r096 train/validation run: 80k steps with cosine 1e-4 to 1e-5." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train data/resolution_transfer/train/r096 \
    --val data/resolution_transfer/val/r096 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --steps 80000 \
    --seed 0 \
    --learning-rate 0.0001 \
    --schedule cosine \
    --warmup-steps 1000 \
    --decay-steps 80000 \
    --cosine-min-learning-rate 0.00001 \
    --log-every 50 \
    --eval-every 500 \
    --val-batches 32 \
    --latest-every 1000 \
    --save-every 20000 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mvp-r096-long80k \
    --wandb-group cross-flowers-mvp-r096-long \
    --wandb-tags cross-flowers mvp r096 full-data overnight long80k
```

Run the printed `pull outputs with:` command locally after completion and
review `training/summary.json`, train/validation curves, per-band errors,
checkpoint artifacts, route diagnostics, GPU utilization, and step time before
evaluating this checkpoint on other resolutions or adding mixed-resolution
training.

### Stage 5: Native multi-resolution validation and mixed-bucket smoke

These runs move from r096 training to the first resolution-transfer evidence
without opening the test split. First, evaluate the exact best r096 checkpoint
on every complete validation bucket at its native input and receiver shape.
Then repeat on the same ordered rows and native targets after conservatively
restricting only the wavespeed input to `96^3`. The difference between those
two runs is the matched fine-input-utilization diagnostic. Because the
production validation buckets contain independent media across resolutions,
this is a distributional resolution-transfer study, not paired solver
convergence evidence; the paired diagnostic dataset remains a separate later
evaluation.

Set the already completed artifact paths on `mutton2`:

```sh
cd ~/3D_Helmholtz
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/20260721_005852_cross-flowers-normalizer-76_e1977484
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
export CROSS_FLOWERS_R096_RUN=outputs/experiments/20260721_033723_cross-flowers-mvp-r096-long80k_f2cc9b40
export CROSS_FLOWERS_R096_BEST="$CROSS_FLOWERS_R096_RUN/training/checkpoints/best.msgpack"
test -f "$CROSS_FLOWERS_NORMALIZER" && test -f "$CROSS_FLOWERS_R096_BEST"
```

Run the native all-validation-row evaluation:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-r096-best-native-multires-val \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Best r096 checkpoint evaluated on all native 96/128/256/512 validation rows; test split remains sealed." \
  -- .venv/bin/python scripts/eval_invariant_flowers.py \
    --checkpoint "$CROSS_FLOWERS_R096_BEST" \
    --config "$CROSS_FLOWERS_R096_RUN/training/config.json" \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --data-bucket r096 data/resolution_transfer/val/r096 \
    --data-bucket r128 data/resolution_transfer/val/r128 \
    --data-bucket r256 data/resolution_transfer/val/r256 \
    --data-bucket r512 data/resolution_transfer/val/r512 \
    --batch-size 1 \
    --panel-row 0 \
    --panel-frequencies-hz 1 5 10 15 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-r096-best-native-multires-val \
    --wandb-group cross-flowers-resolution-transfer-r096 \
    --wandb-tags cross-flowers r096-checkpoint multires validation native-input
```

Run the matched restricted-input control on the identical rows and native
receiver grids:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-r096-best-restrict96-multires-val \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Matched validation control for the best r096 checkpoint: inputs restricted to 96^3, native targets and outputs retained." \
  -- .venv/bin/python scripts/eval_invariant_flowers.py \
    --checkpoint "$CROSS_FLOWERS_R096_BEST" \
    --config "$CROSS_FLOWERS_R096_RUN/training/config.json" \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --data-bucket r096 data/resolution_transfer/val/r096 \
    --data-bucket r128 data/resolution_transfer/val/r128 \
    --data-bucket r256 data/resolution_transfer/val/r256 \
    --data-bucket r512 data/resolution_transfer/val/r512 \
    --input-restrict-to 96 \
    --batch-size 1 \
    --panel-row 0 \
    --panel-frequencies-hz 1 5 10 15 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-r096-best-restrict96-multires-val \
    --wandb-group cross-flowers-resolution-transfer-r096 \
    --wandb-tags cross-flowers r096-checkpoint multires validation restricted-input
```

Each evaluation writes `summary.json`, row/frequency/mode-shell CSV files, two
aggregate plots, and real-pressure panels under the runner's `evaluation/`
directory. Metrics include the normalized training convention and physical-unit
complex diagnostics, with modeled/auxiliary-band and PPW-valid/underresolved
summaries. Pull both runner directories before comparing native-minus-restricted
errors on the same bucket and frequency.

Finally, run a short from-scratch shared-state smoke before committing to a
mixed-resolution overnight run. Each optimizer update contains one homogeneous
bucket, all four shape-specialized call sites update the same parameter and
optimizer state, and equal bucket probabilities match the normalizer. Training
omits the large native receiver target after CPU coefficient projection; native
surface metrics are still computed for validation. Three synchronized
post-compilation steps per bucket provide honest feasibility timings.

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed-r096-r128-r256-r512-smoke100 \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "100-step equal-bucket mixed-resolution shared-state Cross-Flowers compile, memory, timing, and validation smoke." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train-bucket r096 data/resolution_transfer/train/r096 \
    --train-bucket r128 data/resolution_transfer/train/r128 \
    --train-bucket r256 data/resolution_transfer/train/r256 \
    --train-bucket r512 data/resolution_transfer/train/r512 \
    --val-bucket r096 data/resolution_transfer/val/r096 \
    --val-bucket r128 data/resolution_transfer/val/r128 \
    --val-bucket r256 data/resolution_transfer/val/r256 \
    --val-bucket r512 data/resolution_transfer/val/r512 \
    --train-bucket-weight r096=0.25 \
    --train-bucket-weight r128=0.25 \
    --train-bucket-weight r256=0.25 \
    --train-bucket-weight r512=0.25 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --coefficient-only-training \
    --steps 100 \
    --seed 0 \
    --learning-rate 0.0001 \
    --schedule constant \
    --warmup-steps 0 \
    --log-every 10 \
    --eval-every 100 \
    --val-batches 1 \
    --latest-every 0 \
    --save-every 0 \
    --bucket-timing-samples 3 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed-r096-r128-r256-r512-smoke100 \
    --wandb-group cross-flowers-mixed-resolution \
    --wandb-tags cross-flowers multires shared-state smoke feasibility
```

The smoke passes only if every bucket has a nonzero update count, all four
first-compile and steady-step timing records are present, the r512 path fits,
all training/validation metrics are finite, and both `best.msgpack` and
`latest.msgpack` are written. Review this result before setting the length and
schedule of the first full mixed-resolution run.

### Stage 6: First full mixed-resolution MVP

The Stage 5 smoke passed at clean commit `33084a32`: all four buckets compiled,
completed nonzero shared-state updates, remained finite, and wrote matching
best/latest checkpoints. Steady accelerator step times were approximately
`0.50--0.55 s` for every resolution. The first full run uses `240,000` updates
and is expected to take roughly `36--44` hours after randomized input/target
I/O, native validation, and checkpoint overhead are included.

Train from scratch so the effect of the mixed sampling distribution remains
interpretable. Sample in proportion to the `9000/4500/900/180` available
training pairs, equivalently weights `50/25/5/1`. This gives probabilities
`0.617284/0.308642/0.061728/0.012346` and the same expected `16.46` draws per
training pair over 240k steps. The existing resolution-balanced normalizer
remains appropriate: it supplies stable per-frequency scales, while the
relative objective and explicit sampler define the training distribution.
Eight fixed validation batches per bucket retain visibility into every
resolution; aggregate checkpoint selection uses the same proportional bucket
weights. The complete validation buckets are evaluated separately after
training.

```sh
cd ~/3D_Helmholtz
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/20260721_005852_cross-flowers-normalizer-76_e1977484
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
test -f "$CROSS_FLOWERS_NORMALIZER"

CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed-r096-r128-r256-r512-proportional-long240k \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "First full data-proportional mixed-resolution Cross-Flowers run: 240k from-scratch steps with retained checkpoints every 80k." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train-bucket r096 data/resolution_transfer/train/r096 \
    --train-bucket r128 data/resolution_transfer/train/r128 \
    --train-bucket r256 data/resolution_transfer/train/r256 \
    --train-bucket r512 data/resolution_transfer/train/r512 \
    --val-bucket r096 data/resolution_transfer/val/r096 \
    --val-bucket r128 data/resolution_transfer/val/r128 \
    --val-bucket r256 data/resolution_transfer/val/r256 \
    --val-bucket r512 data/resolution_transfer/val/r512 \
    --train-bucket-weight r096=9000 \
    --train-bucket-weight r128=4500 \
    --train-bucket-weight r256=900 \
    --train-bucket-weight r512=180 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --coefficient-only-training \
    --steps 240000 \
    --seed 0 \
    --learning-rate 0.0001 \
    --schedule cosine \
    --warmup-steps 1000 \
    --decay-steps 240000 \
    --cosine-min-learning-rate 0.00001 \
    --log-every 50 \
    --eval-every 1000 \
    --val-batches 8 \
    --latest-every 1000 \
    --save-every 80000 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed-r096-r128-r256-r512-proportional-long240k \
    --wandb-group cross-flowers-mixed-resolution \
    --wandb-tags cross-flowers multires shared-state full-data proportional-sampling long240k
```

The immutable checkpoints at steps 80k, 160k, and 240k enable trajectory
comparisons. The 80k checkpoint is only roughly comparable with the prior 80k
r096-only run: it will contain about `49,383` r096 updates plus updates from the
other resolutions, and its continuous 240k cosine schedule is still near
`7.8e-5` rather than the prior run's terminal `1e-5`. This is deliberate; a
single continuous decay is preferable to spending the final 160k steps pinned
at the minimum learning rate.

After syncing the run, inspect the aggregate and per-bucket validation curves,
realized bucket counts against their multinomial expectations, route
diagnostics, and checkpoint integrity. Then apply
the Stage 5 standalone evaluator to the selected best checkpoint twice: once
with native inputs and once with `--input-restrict-to 96`. That matched pair is
the first test of whether mixed training converts shape-compatible zero-shot
transfer into useful fine-input utilization. Keep the test split sealed.

### Stage 7: Complete validation of the selected mixed checkpoint

The 240k run completed cleanly at commit `33084a32` in `31.75 h`. It realized
`148001/74066/14939/2994` bucket updates and selected step 239k with fixed-panel
aggregate validation loss `0.087424`. All four per-bucket validation losses
also reached their minima at step 239k. Because checkpoint selection used only
eight fixed rows per resolution, run the complete validation evaluator before
comparing to the r096-only checkpoint.

The complete native-input evaluation subsequently succeeded as
`20260723_175710_cross-flowers-mixed240k-best-native-multires-val_33084a32`.
It covered all `500/250/50/10` validation rows and achieved normalized
coefficient relative L2 `0.23292/0.22887/0.20831/0.21442`. These are
`15.8/16.1/17.0/16.7%` below the earlier r096-only checkpoint on the identical
rows. Every row, non-DC frequency bin, and weighted mode shell improved. The
matched restrict-input-to-96 control also completed as
`20260723_181255_cross-flowers-mixed240k-best-restrict96-multires-val_33084a32`.
It changed active physical coefficient relative L2 by only
`+0.185/-0.051/+0.083%` at r128/r256/r512. All auxiliary high-frequency bins
favored native input, but band-level gains remained below `0.9%`, while the
modeled band was neutral or favored restriction at r256/r512. Stage 7 is
complete: the model is a strong shared multi-resolution operator but does not
yet demonstrate material fine-input utilization. Do not open the test split.

Set the completed artifacts on `mutton2`:

```sh
cd ~/3D_Helmholtz
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/20260721_005852_cross-flowers-normalizer-76_e1977484
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
export CROSS_FLOWERS_MIXED_RUN=outputs/experiments/20260721_221249_cross-flowers-mixed-r096-r128-r256-r512-proportional-long240k_33084a32
export CROSS_FLOWERS_MIXED_BEST="$CROSS_FLOWERS_MIXED_RUN/training/checkpoints/best.msgpack"
test -f "$CROSS_FLOWERS_NORMALIZER" && test -f "$CROSS_FLOWERS_MIXED_BEST"
```

First evaluate every validation row with its native input:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed240k-best-native-multires-val \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Selected step-239k proportional mixed-resolution checkpoint evaluated on every native validation row." \
  -- .venv/bin/python scripts/eval_invariant_flowers.py \
    --checkpoint "$CROSS_FLOWERS_MIXED_BEST" \
    --config "$CROSS_FLOWERS_MIXED_RUN/training/config.json" \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --data-bucket r096 data/resolution_transfer/val/r096 \
    --data-bucket r128 data/resolution_transfer/val/r128 \
    --data-bucket r256 data/resolution_transfer/val/r256 \
    --data-bucket r512 data/resolution_transfer/val/r512 \
    --batch-size 1 \
    --panel-row 0 \
    --panel-frequencies-hz 1 5 10 15 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed240k-best-native-multires-val \
    --wandb-group cross-flowers-resolution-transfer-mixed240k \
    --wandb-tags cross-flowers mixed240k multires validation native-input
```

For exact provenance, the completed restricted-input control used the following
command on the identical rows, native targets, and native output grids:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed240k-best-restrict96-multires-val \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Matched control for selected step-239k mixed checkpoint: inputs restricted to 96^3, native targets and outputs retained." \
  -- .venv/bin/python scripts/eval_invariant_flowers.py \
    --checkpoint "$CROSS_FLOWERS_MIXED_BEST" \
    --config "$CROSS_FLOWERS_MIXED_RUN/training/config.json" \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --data-bucket r096 data/resolution_transfer/val/r096 \
    --data-bucket r128 data/resolution_transfer/val/r128 \
    --data-bucket r256 data/resolution_transfer/val/r256 \
    --data-bucket r512 data/resolution_transfer/val/r512 \
    --input-restrict-to 96 \
    --batch-size 1 \
    --panel-row 0 \
    --panel-frequencies-hz 1 5 10 15 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed240k-best-restrict96-multires-val \
    --wandb-group cross-flowers-resolution-transfer-mixed240k \
    --wandb-tags cross-flowers mixed240k multires validation restricted-input
```

Both recorded runs were pulled and inspected. The native result establishes
complete-row accuracy; the paired restricted result establishes only weak,
high-frequency use of information unavailable on the `96^3` input grid.

### Deferred Stage 8: Discarded-detail relevance diagnostic

Status: recommended for a future work session and explicitly not scheduled on
2026-07-23. Do not launch a new long training run or open the test split as part
of this stage.

The purpose is to distinguish three explanations for the small native-input
gain:

1. restriction removes very little wavespeed detail under the current GRF law;
2. removed detail exists but is weakly relevant to the selected surface
   observable and `P=64` output contract; or
3. label-relevant detail exists, but the raw one-channel native Cross memory
   does not use it effectively.

First implement an inexpensive validation-only screening analysis using the
same r128/r256/r512 rows as Stage 7. For each native wavespeed `x_h`, apply the
exact production restriction `R_{h->96}`, prolong it back to the native grid,
and define

```text
d_h = x_h - P_{96->h} R_{h->96} x_h.
```

Record at least:

- absolute detail RMS in physical wavespeed units;
- detail RMS relative to the centered native wavespeed contrast;
- a gradient/seminorm or banded spatial-frequency detail measure;
- paired native gain `error_restricted - error_native` for the full active
  objective, retained `1.0--7.0 Hz` band, high `7.2--15 Hz` band, and
  representative cosine shells.

Report row-matched scatter plots, Spearman and Pearson correlations, quartile
bins, and bootstrap intervals separately by resolution. Treat r512 as
descriptive because it has only 10 validation rows. r096 is the numerical
identity control. Preserve row identifiers, source metadata, input hashes, and
the exact restriction/prolongation metadata. This stage uses validation data
only and does not retrain the model.

The correlation screen cannot by itself establish whether discarded input
detail changes the true response. If it finds negligible detail, do not add a
learned native shell. If it finds substantial detail with no clear native gain,
run a small solver-backed validation counterfactual before changing the
architecture: on selected r128/r256 cases, retain the existing native target
and generate a second native-grid response for
`P_{96->h} R_{h->96} x_h` under the identical source, CPML, frequency, and
receiver contract. The resulting true response difference directly measures
label-relevant fine-input information. A material true difference absent from
the network prediction supports strengthening the native path; a negligible
true difference points instead to the data/observable contract.

No executable Stage 8 command is recorded yet because the screening artifact
and its tests have not been implemented. When this study is resumed, implement
and verify it locally, then package any GPU/solver work through
`scripts/run_experiment.py --require-clean --remote mutton2`.

### Stage 9: 1M-update constant-rate saturation continuation

This user-authorized experiment is independent of deferred Stage 8. Its purpose
is to measure the accuracy floor at constant learning rate `3e-5` and determine
when validation error begins a sustained increase while training error
continues to improve.

Use the selected step-239k checkpoint SHA
`1b92c4d840b9bc0f8f124a5f669b0625744ab8ebee7a82e60ab7d8716365e49e`.
Run 1,000,000 additional optimizer updates, so the trainer's absolute
`--steps` target is 1,239,000. Preserve parameters, AdamW moments, optimizer
count, global step, data, `50/25/5/1` bucket distribution, loss, normalizer,
seed, and batch size. The explicit schedule-change override replaces only the
completed cosine schedule with constant `3e-5` and no warmup. It starts a new
experiment/W&B run rather than appending to the original W&B run.

Immutable checkpoints are measured from the continuation boundary:

| Additional updates | Global step | Checkpoint |
|---:|---:|---|
| 250,000 | 489,000 | `step_00489000.msgpack` |
| 500,000 | 739,000 | `step_00739000.msgpack` |
| 750,000 | 989,000 | `step_00989000.msgpack` |
| 1,000,000 | 1,239,000 | `step_01239000.msgpack` |

`latest.msgpack` is overwritten every 1,000 global steps for recovery.
`best.msgpack` initially preserves the source best and is replaced only by a
lower fixed-panel validation loss. Validation remains eight fixed batches per
resolution every 1,000 steps. At the preceding measured throughput, expect
about `132 h` (`5.5 days`) of trainer time.

The schedule-only continuation support must be committed and checked out
cleanly on `mutton2` before launching. Set and verify the source artifacts:

This continuation intentionally preserves the historical checkpoint's shared
high-band weight `0.1` at every resolution. When `--resume-from` is used
without an explicit `--frequency-loss-weights` vector, the trainer reuses the
originating loss record rather than applying the new fresh-run default.
It also preserves the historical 76-bin output grid and therefore is not a
fresh model under the 71-bin publication contract.

An initial launch from commit `76cd7ab3` was rejected during preflight before
checkpoint restoration or training because the saved JSON lists were compared
directly with equivalent runtime tuples. Do not retry from that commit. The
corrected validator canonicalizes those containers without relaxing any
semantic compatibility check; after pulling the correction, the launch command
below is unchanged.

```sh
cd ~/3D_Helmholtz
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/20260721_005852_cross-flowers-normalizer-76_e1977484
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
export CROSS_FLOWERS_MIXED_RUN=outputs/experiments/20260721_221249_cross-flowers-mixed-r096-r128-r256-r512-proportional-long240k_33084a32
export CROSS_FLOWERS_MIXED_BEST="$CROSS_FLOWERS_MIXED_RUN/training/checkpoints/best.msgpack"
test -f "$CROSS_FLOWERS_NORMALIZER" &&
test -f "$CROSS_FLOWERS_MIXED_BEST" &&
test -f "$CROSS_FLOWERS_MIXED_RUN/training/checkpoints/best.json"
```

Launch manually on `mutton2`:

```sh
cd ~/3D_Helmholtz
CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed239k-constant3e-5-continuation1m \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "One million additional mixed-resolution updates from selected step 239k; preserve AdamW state, switch to constant LR 3e-5, measure saturation and overfitting." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train-bucket r096 data/resolution_transfer/train/r096 \
    --train-bucket r128 data/resolution_transfer/train/r128 \
    --train-bucket r256 data/resolution_transfer/train/r256 \
    --train-bucket r512 data/resolution_transfer/train/r512 \
    --val-bucket r096 data/resolution_transfer/val/r096 \
    --val-bucket r128 data/resolution_transfer/val/r128 \
    --val-bucket r256 data/resolution_transfer/val/r256 \
    --val-bucket r512 data/resolution_transfer/val/r512 \
    --train-bucket-weight r096=9000 \
    --train-bucket-weight r128=4500 \
    --train-bucket-weight r256=900 \
    --train-bucket-weight r512=180 \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --coefficient-only-training \
    --steps 1239000 \
    --seed 0 \
    --optimizer adamw \
    --weight-decay 0.0001 \
    --clip-norm 1.0 \
    --learning-rate 0.00003 \
    --schedule constant \
    --warmup-steps 0 \
    --resume-from "$CROSS_FLOWERS_MIXED_BEST" \
    --allow-resume-schedule-change \
    --log-every 50 \
    --eval-every 1000 \
    --val-batches 8 \
    --latest-every 1000 \
    --save-every 250000 \
    --save-every-relative-to-start \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed239k-constant3e-5-continuation1m \
    --wandb-group cross-flowers-mixed-resolution-long-horizon \
    --wandb-tags cross-flowers multires continuation constant-lr saturation overfit long1m
```

At startup, verify that the trainer prints:

```text
validated optimizer-schedule-only resume override
steps=239000->1239000
```

The first continuation evaluation occurs at global step 240,000. Because the
learning rate jumps from the source cosine floor near `1e-5` to `3e-5`, treat
an early transient separately from persistent overfitting. After completion,
inspect the 1k-step validation trajectory and require a sustained validation
increase alongside improving smoothed training loss before declaring
overfitting. Then run complete all-row validation on the source, four immutable
trajectory checkpoints, continuation best, and final checkpoint as needed.
Keep the test split sealed.

Use the exact `rsync` pull command printed by the runner to synchronize the
completed run directory. The expected directory name begins with
`outputs/experiments/<timestamp>_cross-flowers-mixed239k-constant3e-5-continuation1m_`.

### Stage 10: Fresh 71-bin proportional all-shard 240k run

This is the active fresh-model experiment. It repeats the architecture,
optimizer, 240k cosine schedule, seed, fixed validation panels, and checkpoint
cadence of Stage 6, with exactly three scientific changes:

1. the model sees and outputs only `1.0, 1.2, ..., 15.0 Hz` (`71` bins);
2. `7.2-15.0 Hz` has weight `0.1` at r096/r128 and weight `1` at r256/r512;
3. every training bucket includes the original directory and both completed
   additive shards.

The acoustic arrays remain immutable 76-bin files. The loader selects stored
indices `5..75` before normalization or coefficient projection. The original
train-only normalizer is selected identically in memory, with source/effective
scale hashes and the exact indices written to `config.json`.

Use `--train-bucket-weight-by-size` rather than copied numeric weights. The
trainer counts all rows in the three paths per bucket and records the resulting
probabilities. If the additive manifest matches the planned
`18000/9000/1800/360` addition, the combined counts are
`27000/13500/2700/540`, the resolution probabilities remain
`50/25/5/1` after normalization, and each training row receives about `5.49`
expected draws over 240k updates. If more rows completed, the actual recorded
lengths determine the probabilities automatically.

Run manually on `mutton2` only after committing this implementation and
checking out that clean commit:

```sh
cd ~/3D_Helmholtz
export CROSS_FLOWERS_NORMALIZER_RUN=outputs/experiments/20260721_005852_cross-flowers-normalizer-76_e1977484
export CROSS_FLOWERS_NORMALIZER="$CROSS_FLOWERS_NORMALIZER_RUN/pressure_normalizer_resolution_balanced_76.npz"
test -f "$CROSS_FLOWERS_NORMALIZER"
test -f data/resolution_transfer/manifest.json
test -f data/resolution_transfer_train_add2x_20260724/manifest.json

CUDA_VISIBLE_DEVICES=0 XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name cross-flowers-mixed-r096-r128-r256-r512-proportional-allshards-freq1to15-long240k \
  --config notes/2026_07_19_discretization_invariant_cross_flowers.tex \
  --require-clean \
  --remote mutton2 \
  --notes "Fresh 71-bin Cross-Flowers 240k run using every original/additive training shard, data-proportional bucket sampling, and high-band downweighting only at r096/r128." \
  -- .venv/bin/python scripts/train_invariant_flowers.py \
    --train-bucket r096 \
      data/resolution_transfer/train/r096 \
      data/resolution_transfer_train_add2x_20260724/train/r096_shard0000 \
      data/resolution_transfer_train_add2x_20260724/train/r096_shard0001 \
    --train-bucket r128 \
      data/resolution_transfer/train/r128 \
      data/resolution_transfer_train_add2x_20260724/train/r128_shard0000 \
      data/resolution_transfer_train_add2x_20260724/train/r128_shard0001 \
    --train-bucket r256 \
      data/resolution_transfer/train/r256 \
      data/resolution_transfer_train_add2x_20260724/train/r256_shard0000 \
      data/resolution_transfer_train_add2x_20260724/train/r256_shard0001 \
    --train-bucket r512 \
      data/resolution_transfer/train/r512 \
      data/resolution_transfer_train_add2x_20260724/train/r512_shard0000 \
      data/resolution_transfer_train_add2x_20260724/train/r512_shard0001 \
    --val-bucket r096 data/resolution_transfer/val/r096 \
    --val-bucket r128 data/resolution_transfer/val/r128 \
    --val-bucket r256 data/resolution_transfer/val/r256 \
    --val-bucket r512 data/resolution_transfer/val/r512 \
    --train-bucket-weight-by-size \
    --normalizer "$CROSS_FLOWERS_NORMALIZER" \
    --preset base \
    --decoder cross_flowers \
    --core-grid-sizes 48 24 12 \
    --core-widths 320 640 1280 \
    --integration-shape 96 96 \
    --basis-p 64 \
    --coefficient-output-init-std 0.001 \
    --cross-query-chunk-size 1024 \
    --cross-frequency-chunk-size 4 \
    --batch-size 1 \
    --coefficient-only-training \
    --steps 240000 \
    --seed 0 \
    --learning-rate 0.0001 \
    --schedule cosine \
    --warmup-steps 1000 \
    --decay-steps 240000 \
    --cosine-min-learning-rate 0.00001 \
    --log-every 50 \
    --eval-every 1000 \
    --val-batches 8 \
    --latest-every 1000 \
    --save-every 80000 \
    --wandb \
    --wandb-project Cross-Flowers \
    --wandb-name cross-flowers-mixed-r096-r128-r256-r512-proportional-allshards-freq1to15-long240k \
    --wandb-group cross-flowers-mixed-resolution \
    --wandb-tags cross-flowers multires shared-state all-shards proportional-sampling freq1to15 resolution-specific-frequency-weights long240k
```

Before leaving the run unattended, verify the startup record reports:

- model frequencies `1.0..15.0 Hz` with count `71`;
- selected stored indices `5..75` for all train and validation shards;
- `27000/13500/2700/540` eligible rows if the completed manifest matches the
  planned expansion, or the actual larger counts otherwise;
- high-band weights `0.1/0.1/1.0/1.0` for r096/r128/r256/r512;
- nonzero data-proportional sampling probabilities for every bucket;
- a clean Git commit and W&B run name matching the runner name.

Expected artifacts are under
`outputs/experiments/<timestamp>_cross-flowers-mixed-r096-r128-r256-r512-proportional-allshards-freq1to15-long240k_<sha>/training/`,
including `config.json`, `checkpoints/latest.msgpack`,
`checkpoints/best.msgpack`, and immutable 80k/160k/240k checkpoints. Use the
runner-generated `pull outputs with:` command from the local workstation after
completion.

Historical learned-native-shell results remain available in
`notes/2026_07_19_discretization_invariant_unet_flowers_plan.md`. Their
`--native-width` and `--rematerialize-native-blocks` commands do not apply to
the core-first Cross-Flowers architecture and should not be repeated.

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
