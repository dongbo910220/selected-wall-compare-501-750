# NODE3 路径说明

## 1. 选中的 3 个 case 对照包

目录：

```text
/mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/selected_wall_compare_501_750
```

里面包含：

- `tts_sample_0041_501_750_042`
- `voice_design_sample_0015_501_750_016`
- `voice_clone_tts_sample_0052_501_750_053`

每个 case 文件夹里都有：

- `request.json`
- `concurrent_output.wav`
- `concurrent_meta.json`
- `single_output.wav`
- `single_result.json`
- `README.md`
- `reference.wav`（仅 `voice_clone_tts`）

## 2. 复现实验用的数据素材

parquet：

```text
/mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/selected_wall_compare_501_750/materials/20260429_multi_bin_501_750.parquet
```

这个 repo 里的：

```text
materials/20260429_multi_bin_501_750.parquet
```

只是指向这个文件的软链接。

## 3. 完整 300 条并发实验结果

目录：

```text
/mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/audio_preserve_501_750_300req
```

里面有：

- `raw/`
- `md/`
- `logs/`
- `audio/`

## 4. 运行脚本

repo 里的主脚本：

```text
scripts/run_audio_preserve_501_750.py
```

它默认会：

- 使用 `materials/20260429_multi_bin_501_750.parquet`
- 跑 `tts / voice_design / voice_clone_tts`
- 每个场景 `300` 条
- 固定并发 `6`
- 一条完成补一条
- 每个场景先预热 `1` 条

## 5. 代码与镜像口径

- git ref: `hqx-bluebell-runtimecfg-playbooks-20260430`
- commit: `c714d55b268110d9021dfec52c7cbedaa7418a97`
- image: `ghcr.io/breezebluestudio/vllm-omni-bluebell-v0:v1-with-fa2-cu130-shmfix-runtimecfg-20260428-r2`
