# Materials

这个目录里的 parquet 不直接拷贝进 git。

当前放的是一个指向 `NODE3:/mnt/...` 的符号链接：

```text
materials/20260429_multi_bin_501_750.parquet
  -> /mnt/dongbo_moved/qianxin_commit_c714d55_runtime_20260503/conc300_501_750_probe_20260503_143450/selected_wall_compare_501_750/materials/20260429_multi_bin_501_750.parquet
```

这样同事在 `NODE3` 上 clone 这个 repo 后，主脚本默认就能直接找到素材。
