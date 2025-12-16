"""
TODO: Nested JSON을 flatten하여 Silver Parquet으로 적재
- Challenge #3 (Complex Nested Schema) 처리 및 Schema Evolution 고려
"""

import argparse
import os

from jobs.common.session import get_spark


# 한국어 주석: 스키마 정의/flatten 로직은 이후 채우기


def main():
    parser = argparse.ArgumentParser(
        description="Flatten bronze JSON to silver Parquet"
    )
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "gharchive"))
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--silver-prefix", default="silver")
    _args = parser.parse_args()

    _spark = get_spark("gh-archive-silver")

    # TODO: Challenge #3 스키마 드리프트 대응 및 explode/flatten 구현
    # TODO: partitionBy("dt") 후 Snappy Parquet 저장
    raise NotImplementedError("silver flatten pipeline is not implemented yet")


if __name__ == "__main__":
    main()
