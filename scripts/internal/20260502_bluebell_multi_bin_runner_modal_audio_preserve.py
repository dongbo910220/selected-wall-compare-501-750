import argparse
import asyncio
import importlib.util
import json
import time
import wave
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_RUNNER_PATH = SCRIPT_DIR / "20260430_bluebell_multi_bin_runner_modal.py"
DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "outputs_multi_bin_audio_preserve"
DEFAULT_GPU_LABEL = "unknown"
DEFAULT_PLATFORM_LABEL = "Modal"
PCM_SAMPLE_RATE = 24000
PCM_SAMPLE_WIDTH = 2
NUM_CHANNELS = 1


def load_base_runner():
    spec = importlib.util.spec_from_file_location("bluebell_multi_bin_base", BASE_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load base runner from {BASE_RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load_base_runner()
SCENARIOS = list(base.SCENARIOS)


def write_pcm_as_wav(pcm_bytes: bytes, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(output_path), "wb") as wav_file:
        wav_file.setnchannels(NUM_CHANNELS)
        wav_file.setsampwidth(PCM_SAMPLE_WIDTH)
        wav_file.setframerate(PCM_SAMPLE_RATE)
        wav_file.writeframes(pcm_bytes)


def build_request_context(rows: list[dict], scenario: str, idx: int, model: str) -> tuple[dict, int, dict, dict]:
    row = rows[idx % len(rows)]
    payload, input_chars, sample_meta = base.build_payload(rows, scenario, idx, model)
    context = {
        "input_text": base.clip_text(row.get("text"), base.TEXT_LIMIT),
        "sample_meta": sample_meta,
    }
    if scenario == "voice_design":
        context["instructions"] = base.clip_text(row.get("DSD"), base.INSTRUCT_LIMIT)
    elif scenario == "voice_clone_tts":
        context["reference_text"] = base.clip_text(row.get("reference_text"), base.TEXT_LIMIT)
        context["reference_audio_bytes"] = base.normalize_audio_bytes(row.get("reference_audio"))
    return payload, input_chars, sample_meta, context


def sample_artifact_paths(audio_root: Path, scenario: str, bin_label: str, idx: int) -> dict[str, Path]:
    scenario_dir = audio_root / scenario / bin_label
    base_name = f"sample_{idx:04d}"
    return {
        "scenario_dir": scenario_dir,
        "output_wav": scenario_dir / f"{base_name}_output.wav",
        "meta_json": scenario_dir / f"{base_name}_meta.json",
        "reference_wav": scenario_dir / f"{base_name}_reference.wav",
    }


async def one_request(
    client: httpx.AsyncClient,
    rows: list[dict],
    base_url: str,
    headers: dict[str, str],
    model: str,
    scenario: str,
    idx: int,
    audio_root: Path,
    save_artifacts: bool,
) -> dict:
    url = base_url + "/v1/audio/speech"
    payload, input_chars, sample_meta, context = build_request_context(rows, scenario, idx, model)
    start = time.perf_counter()
    first_chunk = None
    total_bytes = 0
    chunks: list[bytes] = []
    try:
        async with client.stream("POST", url, headers=headers, json=payload) as response:
            status = response.status_code
            if status >= 400:
                body = (await response.aread())[:500].decode("utf-8", "ignore")
                return {
                    "ok": False,
                    "sample_idx": idx,
                    "status": status,
                    "error": body,
                    **sample_meta,
                }
            async for chunk in response.aiter_bytes():
                if not chunk:
                    continue
                if first_chunk is None:
                    first_chunk = time.perf_counter()
                total_bytes += len(chunk)
                if save_artifacts:
                    chunks.append(chunk)
    except Exception as exc:
        return {
            "ok": False,
            "sample_idx": idx,
            "status": "EXC",
            "error": repr(exc),
            **sample_meta,
        }

    end = time.perf_counter()
    if first_chunk is None:
        return {
            "ok": False,
            "sample_idx": idx,
            "status": "EMPTY",
            "error": "stream ended without audio chunk",
            **sample_meta,
        }

    elapsed_sec = end - start
    audio_sec = base.pcm_duration_seconds(total_bytes)
    result = {
        "ok": True,
        "sample_idx": idx,
        "status": 200,
        "ttfa_ms": (first_chunk - start) * 1000,
        "elapsed_sec": elapsed_sec,
        "audio_sec": audio_sec,
        "rtf": elapsed_sec / audio_sec if audio_sec > 0 else None,
        "throughput": audio_sec / elapsed_sec if elapsed_sec > 0 else None,
        "characters_per_sec": input_chars / elapsed_sec if elapsed_sec > 0 else None,
        "input_chars": input_chars,
        "bytes": total_bytes,
        **sample_meta,
    }

    if not save_artifacts:
        return result

    bin_label = sample_meta.get("bin_label") or "unknown_bin"
    artifact_paths = sample_artifact_paths(audio_root, scenario, str(bin_label), idx)
    pcm_bytes = b"".join(chunks)
    write_pcm_as_wav(pcm_bytes, artifact_paths["output_wav"])

    meta_payload = {
        "sample_idx": idx,
        "sample_id": sample_meta.get("sample_id"),
        "bin_label": sample_meta.get("bin_label"),
        "text_char_count": sample_meta.get("text_char_count"),
        "reference_row_id": sample_meta.get("reference_row_id"),
        "scenario": scenario,
        "input_text": context["input_text"],
        "ttfa_ms": result["ttfa_ms"],
        "elapsed_sec": result["elapsed_sec"],
        "audio_sec": result["audio_sec"],
        "rtf": result["rtf"],
        "input_chars": result["input_chars"],
        "status": result["status"],
        "output_wav": str(artifact_paths["output_wav"]),
    }
    if scenario == "voice_design":
        meta_payload["instructions"] = context.get("instructions", "")
    if scenario == "voice_clone_tts":
        reference_wav_path = artifact_paths["reference_wav"]
        reference_wav_path.write_bytes(context["reference_audio_bytes"])
        meta_payload["reference_text"] = context.get("reference_text", "")
        meta_payload["reference_wav"] = str(reference_wav_path)
        result["reference_wav"] = str(reference_wav_path)

    artifact_paths["meta_json"].write_text(
        json.dumps(meta_payload, ensure_ascii=False, indent=2)
    )

    result["output_wav"] = str(artifact_paths["output_wav"])
    result["meta_json"] = str(artifact_paths["meta_json"])
    return result


async def run_warmup(
    rows: list[dict],
    base_url: str,
    headers: dict[str, str],
    model: str,
    scenario: str,
    warmup_count: int,
) -> None:
    if warmup_count <= 0:
        return

    timeout = httpx.Timeout(900.0, read=900.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for warm_idx in range(warmup_count):
            result = await one_request(
                client=client,
                rows=rows,
                base_url=base_url,
                headers=headers,
                model=model,
                scenario=scenario,
                idx=warm_idx,
                audio_root=Path("/tmp"),
                save_artifacts=False,
            )
            if not result.get("ok"):
                raise RuntimeError(
                    f"Warmup failed for scenario={scenario} attempt={warm_idx + 1}: {result}"
                )
            print(
                f"WARMUP_OK scenario={scenario} attempt={warm_idx + 1}/{warmup_count} "
                f"ttfa_ms={result['ttfa_ms']:.1f} audio_sec={result['audio_sec']:.2f}",
                flush=True,
            )


async def run_fixed_concurrency(
    rows: list[dict],
    base_url: str,
    headers: dict[str, str],
    model: str,
    scenario: str,
    concurrency: int,
    audio_root: Path,
) -> dict:
    timeout = httpx.Timeout(900.0, read=900.0)
    started_at = time.perf_counter()
    results = []
    next_idx = 0
    in_flight = {}
    total = len(rows)

    async with httpx.AsyncClient(timeout=timeout) as client:
        while next_idx < total or in_flight:
            while next_idx < total and len(in_flight) < concurrency:
                task = asyncio.create_task(
                    one_request(
                        client=client,
                        rows=rows,
                        base_url=base_url,
                        headers=headers,
                        model=model,
                        scenario=scenario,
                        idx=next_idx,
                        audio_root=audio_root,
                        save_artifacts=True,
                    )
                )
                in_flight[task] = next_idx
                next_idx += 1
            done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                in_flight.pop(task)
                result = task.result()
                results.append(result)
                completed = len(results)
                if completed == total or completed % max(concurrency * 5, 20) == 0:
                    ok_count = sum(1 for item in results if item["ok"])
                    elapsed = time.perf_counter() - started_at
                    print(
                        f"[{scenario}] conc={concurrency} completed={completed}/{total} "
                        f"ok={ok_count} failed={completed - ok_count} elapsed={elapsed:.1f}s",
                        flush=True,
                    )

    elapsed_total = time.perf_counter() - started_at
    return {
        "results": sorted(results, key=lambda item: item["sample_idx"]),
        "wall_elapsed_sec": elapsed_total,
    }


def summarize_results(run_result: dict) -> dict:
    return base.summarize_results(run_result)


def build_markdown(
    platform_label: str,
    base_url: str,
    service_version: str,
    model: str,
    dataset_path: Path,
    dataset_label: str,
    dataset_range: str,
    gpu_label: str,
    row_count: int,
    scenarios: list[str],
    concurrency_levels: list[int],
    summary: dict,
    output_raw_path: Path,
    audio_output_root: Path,
    warmup_count: int,
) -> str:
    lines = []
    lines.append("# 20260502 Bluebell Modal Multi-Bin Benchmark With Audio Preserve")
    lines.append("")
    lines.append(f"- Platform: `{platform_label}`")
    lines.append(f"- Service: `{base_url}/v1/audio/speech`")
    lines.append(f"- Version: `{service_version}`")
    lines.append(f"- Model: `{model}`")
    lines.append(f"- Dataset: `{dataset_path}`")
    lines.append(f"- Dataset bin label: `{dataset_label}`")
    lines.append(f"- Dataset text-length range: `{dataset_range}`")
    lines.append(f"- Rows per scenario per concurrency: `{row_count}`")
    lines.append("- Scenarios: `" + "`, `".join(scenarios) + "`")
    lines.append("- Concurrency: `" + " / ".join(str(item) for item in concurrency_levels) + "`")
    lines.append(f"- GPU label: `{gpu_label}`")
    lines.append("- Request mode: streaming `pcm`")
    lines.append("- Scheduling: fixed target concurrency; submit a new request as soon as one finishes")
    lines.append("- Input truncation: none")
    lines.append(f"- Warmup requests before formal run: `{warmup_count}`")
    lines.append(f"- Audio preserve root: `{audio_output_root}`")
    lines.append("")
    for scenario in scenarios:
        lines.append(f"## {base.scenario_label(scenario)}")
        lines.append("")
        lines.append("| Conc | Requests | Success | Fail | Wall s | TTFA avg | TTFA P50 | TTFA P90 | RTF avg | RTF P50 | RTF P90 | Agg Audio s/s | Agg Chars/s |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for conc in concurrency_levels:
            item = summary[scenario][str(conc)]
            lines.append(
                f"| {conc} | {item['requests']} | {item['success']} | {item['failed']} | "
                f"{base.format_float(item['wall_elapsed_sec'], 1)} | {base.format_ttfa_ms(item['ttfa_ms_avg'])} | "
                f"{base.format_ttfa_ms(item['ttfa_ms_p50'])} | {base.format_ttfa_ms(item['ttfa_ms_p90'])} | "
                f"{base.format_float(item['rtf_avg'], 4)} | {base.format_float(item['rtf_p50'], 4)} | "
                f"{base.format_float(item['rtf_p90'], 4)} | {base.format_float(item['aggregate_audio_seconds_per_sec'], 4)} | "
                f"{base.format_float(item['aggregate_characters_per_sec'], 2)} |"
            )
        lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `voice_clone_tts` uses same-row `text` as target input and same-row `reference_text` / `reference_audio` as the clone pair.")
    lines.append("- `characters` counts only the `input` field length; `instructions`, `ref_text`, and `ref_audio` are not counted.")
    lines.append(f"- Raw benchmark data saved to `{output_raw_path}`.")
    lines.append(f"- Per-sample audio and metadata saved under `{audio_output_root}`.")
    lines.append("")
    return "\n".join(lines)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "--dataset-path", dest="dataset_path", required=True)
    parser.add_argument("--base-url", default=base.DEFAULT_BASE_URL)
    parser.add_argument("--model", default=base.DEFAULT_MODEL)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--gpu-label", default=DEFAULT_GPU_LABEL)
    parser.add_argument("--platform-label", default=DEFAULT_PLATFORM_LABEL)
    parser.add_argument("--raw-out", required=True)
    parser.add_argument("--md-out", required=True)
    parser.add_argument("--audio-out-root", required=True)
    parser.add_argument("--warmup-count", type=int, default=0)
    args = parser.parse_args()

    dataset_path = Path(args.dataset_path)
    raw_out = Path(args.raw_out)
    md_out = Path(args.md_out)
    audio_out_root = Path(args.audio_out_root)
    base_url = args.base_url.rstrip("/")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {args.api_key}"}
    rows = base.load_rows(dataset_path)
    version = await base.fetch_version(base_url, headers)
    scenarios = [args.scenario]
    concurrency_levels = [args.concurrency]
    bin_label = base.dataset_bin_label(rows, dataset_path)
    bin_range = base.dataset_bin_range(rows)

    raw = {
        "service_url": base_url + "/v1/audio/speech",
        "service_version": version,
        "model": args.model,
        "dataset_path": str(dataset_path),
        "dataset_bin_label": bin_label,
        "dataset_bin_range": bin_range,
        "row_count": len(rows),
        "text_limit": base.TEXT_LIMIT,
        "instruct_limit": base.INSTRUCT_LIMIT,
        "gpu_label": args.gpu_label,
        "platform_label": args.platform_label,
        "concurrency_levels": concurrency_levels,
        "scenarios": scenarios,
        "scheduling": "fixed target concurrency; replenish one request when one finishes",
        "audio_output_root": str(audio_out_root),
        "warmup_count": args.warmup_count,
        "results": {},
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    summary = {scenario: {} for scenario in scenarios}

    for scenario in scenarios:
        raw["results"][scenario] = {}
        for conc in concurrency_levels:
            if args.warmup_count > 0:
                print(
                    f"START_WARMUP scenario={scenario} count={args.warmup_count} dataset={dataset_path}",
                    flush=True,
                )
                await run_warmup(
                    rows=rows,
                    base_url=base_url,
                    headers=headers,
                    model=args.model,
                    scenario=scenario,
                    warmup_count=args.warmup_count,
                )
            print(
                f"START scenario={scenario} conc={conc} rows={len(rows)} dataset={dataset_path}",
                flush=True,
            )
            run_result = await run_fixed_concurrency(
                rows=rows,
                base_url=base_url,
                headers=headers,
                model=args.model,
                scenario=scenario,
                concurrency=conc,
                audio_root=audio_out_root,
            )
            raw["results"][scenario][str(conc)] = run_result
            summary[scenario][str(conc)] = summarize_results(run_result)
            print(
                f"DONE scenario={scenario} conc={conc} success={summary[scenario][str(conc)]['success']} "
                f"failed={summary[scenario][str(conc)]['failed']} wall={summary[scenario][str(conc)]['wall_elapsed_sec']:.1f}s",
                flush=True,
            )

    raw_out.parent.mkdir(parents=True, exist_ok=True)
    raw_out.write_text(json.dumps({"raw": raw, "summary": summary}, ensure_ascii=False, indent=2))
    md = build_markdown(
        args.platform_label,
        base_url,
        version,
        args.model,
        dataset_path,
        bin_label,
        bin_range,
        args.gpu_label,
        len(rows),
        scenarios,
        concurrency_levels,
        summary,
        raw_out,
        audio_out_root,
        args.warmup_count,
    )
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(md)
    print(f"WROTE_RAW {raw_out}")
    print(f"WROTE_MD {md_out}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
