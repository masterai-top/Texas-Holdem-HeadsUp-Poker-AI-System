# Reproducibility Checklist

Use this checklist before publishing a MasterAI training or evaluation result.

## Environment

- [ ] Record the exact repository commit.
- [ ] Record operating-system, compiler, Python, CMake, CUDA, PyTorch, Redis, Protobuf, and dependency versions.
- [ ] Record CPU, GPU, memory, storage, thread count, and distributed-training topology.
- [ ] Build in an isolated environment without production credentials or real user data.

## Configuration and data

- [ ] Archive all non-secret configuration used by the run.
- [ ] Record game rules, blinds, stack depth, abstractions, and action sizing.
- [ ] Record random seeds and deterministic settings.
- [ ] Document training data, self-play generation, filtering, and preprocessing.
- [ ] Hash model checkpoints, card abstractions, regret files, and evaluation artifacts.

## Training

- [ ] Record the entry point and complete arguments.
- [ ] Record iterations, samples, optimizer settings, learning rates, and checkpoint cadence.
- [ ] Preserve machine-readable logs for losses, regret, throughput, memory, and wall-clock time.
- [ ] Separate interrupted, resumed, and fresh runs.

## Evaluation

- [ ] Freeze the evaluated policy before the final benchmark.
- [ ] Version and describe every opponent.
- [ ] Rotate seats and use duplicate deals when appropriate.
- [ ] Publish the number of hands or matches and uncertainty estimates.
- [ ] Report failed, excluded, or incomplete runs.
- [ ] Follow [BENCHMARKS.md](BENCHMARKS.md).

## Publication

- [ ] State which results are reproduced, project-reported, estimated, or hypothetical.
- [ ] Publish enough metadata to rerun the experiment without exposing secrets.
- [ ] Cite the repository and exact commit.
- [ ] Document known limitations, negative results, and resource requirements.
