# Scripts

这个目录只放和本次 `501_750 / 300条 / 并发6` 复现直接相关的脚本。

默认假设使用者也在 `NODE3` 上，因此脚本会通过 repo 根目录下的：

```text
materials/20260429_multi_bin_501_750.parquet
```

这个软链接去读取 `/mnt/...` 上的真实数据素材。

## 需要的前提

- 已经有 3 个可用的 Bluebell API server
- 三个场景分别有自己的 base URL
- 这三个服务的代码口径应与本次实验一致：
  - commit `c714d55b268110d9021dfec52c7cbedaa7418a97`
  - image `ghcr.io/breezebluestudio/vllm-omni-bluebell-v0:v1-with-fa2-cu130-shmfix-runtimecfg-20260428-r2`

## 主脚本

- `run_audio_preserve_501_750.py`

这个脚本会：

- 使用 `materials/20260429_multi_bin_501_750.parquet`
- 跑三个场景：`tts` / `voice_design` / `voice_clone_tts`
- 每个场景跑 `300` 条
- 固定并发 `6`
- 有一条完成就补一条
- 每个场景先做 `1` 条预热

## 运行示例

```bash
python3 scripts/run_audio_preserve_501_750.py \
  --tts-base-url http://127.0.0.1:6880 \
  --voice-design-base-url http://127.0.0.1:6881 \
  --voice-clone-base-url http://127.0.0.1:6882
```

## 输出

默认会写到：

```text
selected_wall_compare_501_750/repro_outputs/audio_preserve_501_750_300req
```

其中包括：

- `raw/`
- `md/`
- `logs/`
- `audio/`

## internal/

`internal/` 里放的是主脚本依赖的 runner 原件，主要是为了保证别人把这个目录整体拷走之后，仍然能直接跑，不需要再回头找我们机器上的其他路径。
