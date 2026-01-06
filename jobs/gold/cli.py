"""
골드 집계 CLI/날짜 처리
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os

UTC = dt.timezone.utc


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
    if verbose:
        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)


def parse_date(text: str) -> dt.date:
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--date 형식 오류: {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate silver to gold marts (top repos, event types)"
    )
    path_group = parser.add_argument_group("경로")
    path_group.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "gharchive"))
    path_group.add_argument("--silver-prefix", default="silver")
    path_group.add_argument("--gold-prefix", default="gold")

    date_group = parser.add_argument_group("날짜")
    date_range = date_group.add_mutually_exclusive_group()
    date_range.add_argument(
        "--date", type=parse_date, help="UTC 기준 단일 날짜 yyyy-mm-dd"
    )
    date_range.add_argument(
        "--start-date",
        type=parse_date,
        help="UTC 기준 시작 날짜 yyyy-mm-dd",
    )
    date_group.add_argument(
        "--end-date",
        type=parse_date,
        help="UTC 기준 종료 날짜 yyyy-mm-dd (미지정 시 start-date 사용)",
    )

    repo_group = parser.add_argument_group("repo 집계")
    repo_group.add_argument("--top-n", type=int, default=10, help="Top repos 개수")
    repo_group.add_argument(
        "--top-event-types", type=int, default=20, help="상위 이벤트 타입 개수"
    )
    repo_group.add_argument(
        "--daily-top-n",
        type=int,
        default=10,
        help="일별 Top repos 개수(0이면 비활성화)",
    )

    actor_group = parser.add_argument_group("actor 집계")
    actor_group.add_argument(
        "--top-actors", type=int, default=10, help="Top actors 개수"
    )
    actor_group.add_argument(
        "--actor-metric",
        choices=["event_count", "distinct_repo"],
        default="event_count",
        help="actor 집계 지표(event_count/distinct_repo)",
    )

    output_group = parser.add_argument_group("출력/디버그")
    output_group.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="골드 출력 포맷",
    )
    output_group.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="골드 출력 파일 개수(작은 파일 완화)",
    )
    output_group.add_argument(
        "--explain",
        action="store_true",
        help="물리 실행 계획 출력",
    )
    output_group.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG 로그 활성화",
    )
    return parser.parse_args()


def resolve_date_range(args: argparse.Namespace) -> tuple[dt.date, dt.date]:
    if args.date:
        return args.date, args.date
    if args.start_date:
        return args.start_date, args.end_date or args.start_date
    if args.end_date:
        raise ValueError("--end-date는 --start-date와 함께 사용해야 합니다")
    yesterday = (dt.datetime.now(tz=UTC) - dt.timedelta(days=1)).date()
    return yesterday, yesterday
