# Benchmark Reporting Protocol

This document defines the minimum information required for a reproducible MasterAI poker-AI benchmark.

## Status of previously reported results

The previous README reported win rates of 85% against a random bot, 72% against a rule-based AI, and 58% against a CFR baseline. Those values have not been independently reproduced as part of this documentation update and should be treated as project-reported snapshots until complete methodology and raw results are published.

## Required experiment metadata

Every result should record:

- Repository commit hash and whether the worktree was modified
- Operating system, compiler, Python, CUDA, framework, Redis, and dependency versions
- CPU, GPU, RAM, storage, and thread count
- Configuration files and command-line arguments
- Random seeds and deterministic settings
- Game variant, blinds, stack depth, action abstraction, and bet sizing
- Opponent name, version, implementation, and whether it adapts during evaluation
- Number of hands, matches, duplicate deals, and seat rotation protocol
- Evaluation metric, uncertainty estimate, and confidence interval
- Wall-clock time, decision latency, memory use, and model size
- Links or hashes for models, logs, and result artifacts

## Recommended poker metrics

- Milli-big-blinds per game (mbb/g) or big blinds per 100 hands (bb/100)
- Mean and median decision latency
- 95% confidence interval or another justified uncertainty measure
- Exploitability or approximate best-response metrics when available
- Peak and steady-state memory
- Training throughput and total compute budget

A raw win percentage can be misleading when pots and stack sizes vary. Prefer value-based metrics and publish the exact aggregation method.

## Result template

```text
Commit:
Model artifact hash:
Game and stack depth:
Opponent and version:
Hands / duplicate matches:
Seeds:
Hardware:
Configuration:
Primary metric:
Estimate and 95% interval:
Mean / p95 decision latency:
Peak memory:
Raw log location:
Known limitations:
```

## Comparison rules

Do not compare systems unless the game rules, stack depth, action space, opponent, seat protocol, hand sample, and metric are aligned. Clearly separate training-set, validation, and final evaluation results.
