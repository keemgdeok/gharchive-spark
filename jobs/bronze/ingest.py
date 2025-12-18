"""
GHArchive 브론즈 수집기 (CLI)
- 비동기 다운로드, 재시도, 멱등 업로드
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import os
import sys

import aiohttp

from jobs.bronze.pipeline import IngestConfig, build_job, run_pipeline, summarize
from jobs.bronze.storage import build_s3_client, ensure_bucket

# 기본 설정 (환경 변수로 override 가능)
DEFAULT_BASE_URL = os.getenv("GH_ARCHIVE_BASE_URL", "https://data.gharchive.org")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "gharchive")
DEFAULT_PREFIX = os.getenv("BRONZE_PREFIX", "bronze")
DEFAULT_CONCURRENCY = int(os.getenv("INGEST_CONCURRENCY", "8"))
DEFAULT_HTTP_ATTEMPTS = int(os.getenv("INGEST_HTTP_ATTEMPTS", "4"))
DEFAULT_TIMEOUT = int(os.getenv("INGEST_HTTP_TIMEOUT", "300"))
UTC = dt.timezone.utc


def configure_logging(verbose: bool) -> None:
    # 로깅 레벨을 실행 시점에 조정
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def parse_hour(text: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--hour 형식 오류: {exc}") from exc


def resolve_hours(date_text: str) -> list[dt.datetime]:
    try:
        base = dt.datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--date 형식 오류: {exc}") from exc
    return [base.replace(hour=h) for h in range(24)]


def parse_args() -> tuple[IngestConfig, list[dt.datetime], bool]:
    parser = argparse.ArgumentParser(
        description="GHArchive .json.gz를 비동기로 다운로드해 MinIO bronze에 적재 (기본 하루 24시간)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--date",
        required=False,
        help="UTC 기준 단일 날짜 yyyy-mm-dd (24시간 전체 처리). 미지정 시 어제(UTC) 사용.",
    )
    group.add_argument(
        "--hour",
        type=parse_hour,
        required=False,
        help="UTC 기준 단일 시간 yyyy-mm-dd-HH",
    )

    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="동시 다운로드/업로드 개수",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="HTTP 총 타임아웃(초)",
    )
    parser.add_argument(
        "--http-attempts",
        type=int,
        default=DEFAULT_HTTP_ATTEMPTS,
        help="HTTP 재시도 횟수",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="이미 존재하는 오브젝트는 건너뜀(멱등)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG 로그 활성화",
    )

    args = parser.parse_args()
    if args.hour is not None:
        hours = [args.hour]
    else:
        target_date = args.date or (
            dt.datetime.now(tz=UTC) - dt.timedelta(days=1)
        ).strftime("%Y-%m-%d")
        hours = resolve_hours(target_date)

    cfg = IngestConfig(
        base_url=args.base_url,
        bucket=DEFAULT_BUCKET,
        prefix=DEFAULT_PREFIX.rstrip("/"),
        concurrency=max(1, args.concurrency),
        skip_existing=args.skip_existing,
        http_attempts=max(1, args.http_attempts),
        timeout=aiohttp.ClientTimeout(total=args.timeout),
    )
    return cfg, hours, args.verbose


def main() -> None:
    cfg, hours, verbose = parse_args()
    configure_logging(verbose)

    jobs = [build_job(h, cfg.base_url, cfg.prefix) for h in hours]
    logging.info("총 %d개 시간대 처리 예정", len(jobs))

    client = build_s3_client()
    ensure_bucket(client, cfg.bucket, True)

    results = asyncio.run(run_pipeline(jobs, client, cfg))
    summary = summarize(results)
    logging.info(
        "완료 ok=%d skipped=%d failed=%d",
        summary["ok"],
        summary["skipped"],
        summary["failed"],
    )
    if summary["failed"] > 0:
        logging.error("일부 시간대 실패")
        sys.exit(1)


if __name__ == "__main__":
    main()
