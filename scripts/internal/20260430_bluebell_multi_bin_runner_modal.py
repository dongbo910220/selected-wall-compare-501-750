import argparse
import asyncio
import base64
import json
import statistics
import time
from pathlib import Path

import httpx
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "outputs_multi_bin"
DEFAULT_BASE_URL = "http://127.0.0.1:8092"
DEFAULT_MODEL = "bluebell-v1-en-small"
DEFAULT_CONCURRENCY_LEVELS = [6]
SCENARIOS = ["tts", "voice_design", "voice_clone_tts"]
DEFAULT_GPU_LABEL = "unknown"
DEFAULT_PLATFORM_LABEL = "Modal"
PCM_SAMPLE_RATE = 24000
PCM_SAMPLE_WIDTH = 2
TEXT_LIMIT = None
INSTRUCT_LIMIT = None
RAW_OUT = DEFAULT_OUTPUT_ROOT / "tmp/20260430_modal_multi_bin_raw.json"
MD_OUT = DEFAULT_OUTPUT_ROOT / "doc/20260430_modal_multi_bin_tables.md"


def pcm_duration_seconds(num_bytes: int) -> float:
    return num_bytes / PCM_SAMPLE_RATE / PCM_SAMPLE_WIDTH


def format_float(value, digits=4):
    if value is None:
        return "--"
    return f"{value:.{digits}f}"


def format_ttfa_ms(value):
    if value is None:
        return "--"
    return f"{value:.1f}ms"


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = pos - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def clip_text(value: str | None, limit: int | None) -> str:
    text = value or ""
    if limit is None or limit <= 0:
        return text
    return text[:limit]


def load_rows(dataset_path: Path) -> list[dict]:
    return pq.read_table(
        dataset_path,
        columns=[
            "id",
            "bin_label",
            "bin_min_len",
            "bin_max_len",
            "text",
            "text_char_count",
            "DSD",
            "reference_text",
            "reference_audio",
            "reference_row_id",
        ],
    ).to_pylist()


def normalize_audio_bytes(raw_audio_field) -> bytes:
    raw_audio = raw_audio_field
    if isinstance(raw_audio_field, dict):
        raw_audio = raw_audio_field.get("bytes")
    if isinstance(raw_audio, memoryview):
        raw_audio = raw_audio.tobytes()
    if isinstance(raw_audio, bytearray):
        raw_audio = bytes(raw_audio)
    if not isinstance(raw_audio, bytes):
        raise TypeError(f"Unsupported reference_audio payload type: {type(raw_audio)!r}")
    return raw_audio


def dataset_bin_label(rows: list[dict], dataset_path: Path) -> str:
    labels = {row.get("bin_label") for row in rows if row.get("bin_label")}
    if len(labels) == 1:
        return next(iter(labels))
    if len(labels) > 1:
        return ",".join(sorted(labels))
    return dataset_path.stem


def dataset_bin_range(rows: list[dict]) -> str:
    mins = {row.get("bin_min_len") for row in rows if row.get("bin_min_len") is not None}
    maxs = {row.get("bin_max_len") for row in rows if row.get("bin_max_len") is not None}
    if len(mins) == 1 and len(maxs) == 1:
        return f"{next(iter(mins))}-{next(iter(maxs))}"
    return "unknown"


def build_payload(rows: list[dict], scenario: str, idx: int, model: str) -> tuple[dict, int, dict]:
    row = rows[idx % len(rows)]
    text = clip_text(row["text"], TEXT_LIMIT)

    sample_meta = {
        "sample_id": row.get("id"),
        "bin_label": row.get("bin_label"),
        "text_char_count": row.get("text_char_count"),
        "reference_row_id": row.get("reference_row_id"),
    }

    if scenario == "tts":
        payload = {
            "model": model,
            "input": text,
            "voice": "vivian",
            "response_format": "pcm",
            "stream": True,
        }
        return payload, len(text), sample_meta

    if scenario == "voice_design":
        instructions = clip_text(row.get("DSD"), INSTRUCT_LIMIT)
        payload = {
            "model": model,
            "input": text,
            "instructions": instructions,
            "response_format": "pcm",
            "stream": True,
        }
        return payload, len(text), sample_meta

    if scenario == "voice_clone_tts":
        raw_audio = normalize_audio_bytes(row.get("reference_audio"))
        ref_audio = "data:audio/wav;base64," + base64.b64encode(raw_audio).decode()
        ref_text = clip_text(row.get("reference_text"), TEXT_LIMIT)
        payload = {
            "model": model,
            "task_type": "Base",
            "input": text,
            "voice": "vivian",
            "ref_audio": ref_audio,
            "ref_text": ref_text,
            "response_format": "pcm",
            "stream": True,
        }
        return payload, len(text), sample_meta

    raise ValueError(f"Unsupported scenario: {scenario!r}")


async def one_request(
    client: httpx.AsyncClient,
    rows: list[dict],
    base_url: str,
    headers: dict[str, str],
    model: str,
    scenario: str,
    idx: int,
) -> dict:
    url = base_url + "/v1/audio/speech"
    payload, input_chars, sample_meta = build_payload(rows, scenario, idx, model)
    start = time.perf_counter()
    first_chunk = None
    total_bytes = 0
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
    audio_sec = pcm_duration_seconds(total_bytes)
    return {
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


async def run_fixed_concurrency(
    rows: list[dict],
    base_url: str,
    headers: dict[str, str],
    model: str,
    scenario: str,
    concurrency: int,
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
                    one_request(client, rows, base_url, headers, model, scenario, next_idx)
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
    results = run_result["results"]
    ok = [item for item in results if item["ok"]]
    failed = [item for item in results if not item["ok"]]
    codes = {}
    for item in results:
        codes[str(item["status"])] = codes.get(str(item["status"]), 0) + 1
    ttfa_values = [item["ttfa_ms"] for item in ok]
    rtf_values = [item["rtf"] for item in ok]
    throughput_values = [item["throughput"] for item in ok]
    chars_values = [item["characters_per_sec"] for item in ok]
    audio_seconds = sum(item["audio_sec"] for item in ok)
    input_chars = sum(item["input_chars"] for item in ok)
    wall_elapsed = run_result["wall_elapsed_sec"]
    aggregate_chars_per_sec = input_chars / wall_elapsed if wall_elapsed > 0 else None
    aggregate_audio_seconds_per_sec = audio_seconds / wall_elapsed if wall_elapsed > 0 else None
    rtf_avg = statistics.mean(rtf_values) if rtf_values else None
    ttfa_avg = statistics.mean(ttfa_values) if ttfa_values else None
    return {
        "requests": len(results),
        "success": len(ok),
        "failed": len(failed),
        "success_rate": len(ok) / len(results) if results else 0.0,
        "codes": codes,
        "wall_elapsed_sec": wall_elapsed,
        "total_audio_sec": audio_seconds,
        "total_input_chars": input_chars,
        "ttfa_ms_avg": ttfa_avg,
        "ttfa_ms_p50": percentile(ttfa_values, 0.50),
        "ttfa_ms_p90": percentile(ttfa_values, 0.90),
        "ttfa_ms_p99": percentile(ttfa_values, 0.99),
        "rtf_avg": rtf_avg,
        "rtf_p50": percentile(rtf_values, 0.50),
        "rtf_p90": percentile(rtf_values, 0.90),
        "rtf_p99": percentile(rtf_values, 0.99),
        "throughput_avg": statistics.mean(throughput_values) if throughput_values else None,
        "characters_per_sec_avg": statistics.mean(chars_values) if chars_values else None,
        "aggregate_audio_seconds_per_sec": aggregate_audio_seconds_per_sec,
        "aggregate_characters_per_sec": aggregate_chars_per_sec,
    }


def scenario_label(name: str) -> str:
    return {
        "tts": "TTS",
        "voice_design": "Voice Design",
        "voice_clone_tts": "Voice Clone TTS",
    }[name]


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
) -> str:
    lines = []
    lines.append("# 20260430 Bluebell Modal Multi-Bin Benchmark")
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
    lines.append("- Audio duration formula: `bytes / (24000 * 2)`")
    lines.append("- Input truncation: none")
    lines.append("")
    for scenario in scenarios:
        lines.append(f"## {scenario_label(scenario)}")
        lines.append("")
        lines.append("| Conc | Requests | Success | Fail | Wall s | TTFA avg | TTFA P50 | TTFA P90 | RTF avg | RTF P50 | RTF P90 | Agg Audio s/s | Agg Chars/s |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for conc in concurrency_levels:
            item = summary[scenario][str(conc)]
            lines.append(
                f"| {conc} | {item['requests']} | {item['success']} | {item['failed']} | "
                f"{format_float(item['wall_elapsed_sec'], 1)} | {format_ttfa_ms(item['ttfa_ms_avg'])} | "
                f"{format_ttfa_ms(item['ttfa_ms_p50'])} | {format_ttfa_ms(item['ttfa_ms_p90'])} | "
                f"{format_float(item['rtf_avg'], 4)} | {format_float(item['rtf_p50'], 4)} | "
                f"{format_float(item['rtf_p90'], 4)} | {format_float(item['aggregate_audio_seconds_per_sec'], 4)} | "
                f"{format_float(item['aggregate_characters_per_sec'], 2)} |"
            )
        lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `voice_clone_tts` uses same-row `text` as target input and same-row `reference_text` / `reference_audio` as the clone pair.")
    lines.append("- `characters` counts only the `input` field length; `instructions`, `ref_text`, and `ref_audio` are not counted.")
    lines.append(f"- Raw benchmark data saved to `{output_raw_path}`.")
    lines.append("")
    return "\n".join(lines)


async def fetch_version(base_url: str, headers: dict[str, str]) -> str:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=15.0)) as client:
            response = await client.get(base_url + "/version", headers=headers)
            response.raise_for_status()
            return response.json().get("version", "unknown")
    except Exception as exc:
        return f"unknown ({type(exc).__name__})"


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "--dataset-path", dest="dataset_path", required=True)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--scenario", choices=SCENARIOS, default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--gpu-label", default=DEFAULT_GPU_LABEL)
    parser.add_argument("--platform-label", default=DEFAULT_PLATFORM_LABEL)
    parser.add_argument("--raw-out", required=True)
    parser.add_argument("--md-out", required=True)
    args = parser.parse_args()

    dataset_path = Path(args.dataset_path)
    raw_out = Path(args.raw_out)
    md_out = Path(args.md_out)
    base_url = args.base_url.rstrip("/")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {args.api_key}"}
    rows = load_rows(dataset_path)
    version = await fetch_version(base_url, headers)
    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    concurrency_levels = [args.concurrency] if args.concurrency else list(DEFAULT_CONCURRENCY_LEVELS)
    bin_label = dataset_bin_label(rows, dataset_path)
    bin_range = dataset_bin_range(rows)

    raw = {
        "service_url": base_url + "/v1/audio/speech",
        "service_version": version,
        "model": args.model,
        "dataset_path": str(dataset_path),
        "dataset_bin_label": bin_label,
        "dataset_bin_range": bin_range,
        "row_count": len(rows),
        "text_limit": TEXT_LIMIT,
        "instruct_limit": INSTRUCT_LIMIT,
        "gpu_label": args.gpu_label,
        "platform_label": args.platform_label,
        "concurrency_levels": concurrency_levels,
        "scenarios": scenarios,
        "scheduling": "fixed target concurrency; replenish one request when one finishes",
        "results": {},
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    summary = {scenario: {} for scenario in scenarios}

    for scenario in scenarios:
        raw["results"][scenario] = {}
        for conc in concurrency_levels:
            print(
                f"START scenario={scenario} conc={conc} rows={len(rows)} dataset={dataset_path}",
                flush=True,
            )
            run_result = await run_fixed_concurrency(
                rows, base_url, headers, args.model, scenario, conc
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
    )
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(md)
    print(f"WROTE_RAW {raw_out}")
    print(f"WROTE_MD {md_out}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
