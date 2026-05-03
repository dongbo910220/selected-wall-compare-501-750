#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = ROOT / "materials" / "20260429_multi_bin_501_750.parquet"
DEFAULT_OUTPUT_ROOT = ROOT / "repro_outputs" / "audio_preserve_501_750_300req"
DEFAULT_RUNNER = Path(__file__).resolve().parent / "internal" / "20260502_bluebell_multi_bin_runner_modal_audio_preserve.py"
SCENARIOS = ["tts", "voice_design", "voice_clone_tts"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run the 501_750 / 300-request audio-preserve benchmark against three already-running servers."
    )
    p.add_argument("--tts-base-url", required=True)
    p.add_argument("--voice-design-base-url", required=True)
    p.add_argument("--voice-clone-base-url", required=True)
    p.add_argument("--dataset", default=str(DEFAULT_DATASET))
    p.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    p.add_argument("--runner-path", default=str(DEFAULT_RUNNER))
    p.add_argument("--model", default="bluebell-v1-en-small")
    p.add_argument("--api-key", default="sk-llm-infer")
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--warmup-count", type=int, default=1)
    p.add_argument("--platform-label", default="qianxin-c714d55-runtime")
    return p


def main() -> int:
    args = build_parser().parse_args()

    dataset = Path(args.dataset).resolve()
    if not dataset.exists():
        raise SystemExit(f"dataset not found: {dataset}")

    runner = Path(args.runner_path).resolve()
    if not runner.exists():
        raise SystemExit(f"runner not found: {runner}")

    output_root = Path(args.output_root).resolve()
    raw_dir = output_root / "raw"
    md_dir = output_root / "md"
    logs_dir = output_root / "logs"
    audio_root = output_root / "audio"
    for d in [raw_dir, md_dir, logs_dir, audio_root]:
        d.mkdir(parents=True, exist_ok=True)

    entries = [
        {"gpu_id": "tts", "base_url": args.tts_base_url},
        {"gpu_id": "voice_design", "base_url": args.voice_design_base_url},
        {"gpu_id": "voice_clone_tts", "base_url": args.voice_clone_base_url},
    ]

    procs = []
    commands = []

    for scenario, entry in zip(SCENARIOS, entries):
        raw_out = raw_dir / f"{scenario}_501_750_raw.json"
        md_out = md_dir / f"{scenario}_501_750.md"
        log_out = logs_dir / f"{scenario}_501_750.log"
        cmd = [
            sys.executable,
            str(runner),
            "--dataset",
            str(dataset),
            "--base-url",
            entry["base_url"],
            "--model",
            args.model,
            "--scenario",
            scenario,
            "--concurrency",
            str(args.concurrency),
            "--api-key",
            args.api_key,
            "--gpu-label",
            f"GPU {entry['gpu_id']}",
            "--platform-label",
            args.platform_label,
            "--raw-out",
            str(raw_out),
            "--md-out",
            str(md_out),
            "--audio-out-root",
            str(audio_root),
            "--warmup-count",
            str(args.warmup_count),
        ]
        commands.append(" ".join(cmd))
        fh = log_out.open("w", encoding="utf-8")
        procs.append((scenario, subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT), fh))

    (output_root / "run_commands.txt").write_text("\n".join(commands) + "\n", encoding="utf-8")

    failures = []
    for scenario, proc, fh in procs:
        rc = proc.wait()
        fh.close()
        if rc != 0:
            failures.append((scenario, rc))

    if failures:
        formatted = ", ".join(f"{scenario}:{rc}" for scenario, rc in failures)
        raise SystemExit(f"one or more scenarios failed: {formatted}")

    print(f"Completed audio-preserve run at: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
