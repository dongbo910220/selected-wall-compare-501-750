# selected_wall_compare_501_750 repo

这个 repo 是给同事复现 `501_750 / 300条 / 并发6` 实验用的轻量交接包。

它只放两类东西：

- 可直接复现的方法与脚本
- 指向 `NODE3` 上 `/mnt` 真实素材和结果的路径说明

不把大体积音频结果直接塞进 git，避免包太重、版本管理太乱。

## 这次对齐的代码与镜像口径

- git ref: `hqx-bluebell-runtimecfg-playbooks-20260430`
- commit: `c714d55b268110d9021dfec52c7cbedaa7418a97`
- image: `ghcr.io/breezebluestudio/vllm-omni-bluebell-v0:v1-with-fa2-cu130-shmfix-runtimecfg-20260428-r2`

## 目录说明

- `scripts/`
  - 复现实验主脚本
  - 以及它依赖的 runner 原件
- `materials/`
  - 指向 `NODE3` 上 parquet 素材的符号链接
- `selection_attempts.json`
  - 我们从 `300` 条结果里挑出 3 个代表性 case 的记录
- `NODE3_PATHS.md`
  - 人读版路径说明
- `node3_paths.json`
  - 机器可读版路径说明
- `node3_artifacts/`
  - 指向 `NODE3` 上真实结果目录的符号链接

## 最推荐的阅读顺序

1. 先看 `NODE3_PATHS.md`
2. 再看 `scripts/README.md`
3. 然后跑：

```bash
python3 scripts/run_audio_preserve_501_750.py \
  --tts-base-url http://127.0.0.1:6880 \
  --voice-design-base-url http://127.0.0.1:6881 \
  --voice-clone-base-url http://127.0.0.1:6882
```

## NODE3 上最重要的真实结果目录

- 选中的 3 个对照 case：
  - `/mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/selected_wall_compare_501_750`
- 完整 `300条 / 三场景 / 并发6` 原始结果：
  - `/mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/audio_preserve_501_750_300req`

## 说明

这个 repo 默认假设使用者也在 `NODE3` 上，因此：

- `materials/20260429_multi_bin_501_750.parquet`
- `node3_artifacts/*`

都是指向 `NODE3:/mnt/...` 的符号链接。
