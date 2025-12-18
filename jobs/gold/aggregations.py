"""
TODO: Silver 데이터로 Gold 마트 생성
- Challenge #1 (Data Skew) 재현 및 Salting/Broadcast Join 적용
- Challenge #2 (Small File) coalesce(1) 또는 적정 파티션 병합
"""

import argparse
import os

from jobs.spark_runtime import get_spark


# 집계/튜닝 로직은 이후 채우기


def main():
    parser = argparse.ArgumentParser(description="Aggregate silver to gold mart")
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "gharchive"))
    parser.add_argument("--silver-prefix", default="silver")
    parser.add_argument("--gold-prefix", default="gold")
    _args = parser.parse_args()

    _spark = get_spark("gh-archive-gold")

    # TODO: Challenge #1 쏠림 재현 및 Salting/Broadcast Join 비교
    # TODO: Challenge #2 coalesce/repartition으로 작은 파일 해소 후 저장
    # TODO: 결과를 단일 CSV/Parquet로 저장해 BI 용이성 확보
    raise NotImplementedError("gold aggregation pipeline is not implemented yet")


if __name__ == "__main__":
    main()
