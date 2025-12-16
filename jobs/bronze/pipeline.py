"""
GHArchive 브론즈 적재 파이프라인
- 시간대 단위 작업 실행/결과 요약
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass

import aiohttp

from jobs.bronze.downloader import download_bytes_with_retry
from jobs.bronze.storage import object_exists, upload_bytes

UTC = dt.timezone.utc


@dataclass(frozen=True)
class IngestConfig:
    base_url: str
    bucket: str
    prefix: str
    concurrency: int
    skip_existing: bool
    http_attempts: int
    timeout: aiohttp.ClientTimeout


@dataclass(frozen=True)
class HourlyJob:
    hour: dt.datetime
    url: str
    key: str


@dataclass(frozen=True)
class IngestResult:
    hour: dt.datetime
    key: str
    url: str
    status: str
    size_bytes: int
    error: str | None = None
    elapsed_sec: float | None = None


def build_job(hour: dt.datetime, base_url: str, prefix: str) -> HourlyJob:
    normalized = hour.astimezone(UTC)
    date_str = normalized.strftime("%Y-%m-%d")
    url = f"{base_url.rstrip('/')}/{date_str}-{normalized.hour}.json.gz"
    key = f"{prefix.rstrip('/')}/{normalized:%Y/%m/%d}/{date_str}-{normalized.hour}.json.gz"
    return HourlyJob(hour=normalized, url=url, key=key)


async def process_hour(
    job: HourlyJob,
    session: aiohttp.ClientSession,
    client,
    cfg: IngestConfig,
) -> IngestResult:
    start_time = asyncio.get_running_loop().time()
    try:
        if cfg.skip_existing:
            exists = await object_exists(client, cfg.bucket, job.key)
            if exists:
                logging.info("SKIP %s (exists)", job.key)
                return IngestResult(
                    hour=job.hour,
                    key=job.key,
                    url=job.url,
                    status="skipped",
                    size_bytes=0,
                    elapsed_sec=0.0,
                )

        payload = await download_bytes_with_retry(
            session=session,
            url=job.url,
            attempts=cfg.http_attempts,
            timeout=cfg.timeout,
        )
        size = len(payload)
        if size == 0:
            raise ValueError("다운로드 결과가 비어 있습니다")

        await upload_bytes(client, cfg.bucket, job.key, payload)
        elapsed = asyncio.get_running_loop().time() - start_time
        logging.info("OK %s (%d bytes, %.2fs)", job.key, size, elapsed)
        return IngestResult(
            hour=job.hour,
            key=job.key,
            url=job.url,
            status="ok",
            size_bytes=size,
            elapsed_sec=elapsed,
        )
    except FileNotFoundError as exc:
        logging.error("404 %s (%s)", job.url, exc)
        return IngestResult(
            hour=job.hour,
            key=job.key,
            url=job.url,
            status="failed",
            size_bytes=0,
            error=str(exc),
            elapsed_sec=asyncio.get_running_loop().time() - start_time,
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("FAILED %s: %s", job.url, exc)
        return IngestResult(
            hour=job.hour,
            key=job.key,
            url=job.url,
            status="failed",
            size_bytes=0,
            error=str(exc),
            elapsed_sec=asyncio.get_running_loop().time() - start_time,
        )


async def run_pipeline(
    jobs: list[HourlyJob],
    client,
    cfg: IngestConfig,
) -> list[IngestResult]:
    concurrency = max(1, cfg.concurrency)
    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(concurrency)

        async def bound_process(job: HourlyJob) -> IngestResult:
            async with sem:
                return await process_hour(job, session, client, cfg)

        tasks = [asyncio.create_task(bound_process(job)) for job in jobs]
        return await asyncio.gather(*tasks)


def summarize(results: list[IngestResult]) -> dict[str, int]:
    summary = {"ok": 0, "skipped": 0, "failed": 0}
    for r in results:
        summary[r.status] = summary.get(r.status, 0) + 1
    return summary
