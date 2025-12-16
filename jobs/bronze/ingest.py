"""
TODO: GHArchive .json.gz를 비동기 다운로드 후 MinIO bronze 계층에 업로드
- Challenge #2 (Small File) 재현 및 측정 코드 포함 예정
"""

import argparse
import os


# 한국어 주석: 실제 로직은 추후 구현. 구조만 우선 생성.


def main():
    parser = argparse.ArgumentParser(
        description="Download GHArchive and write to MinIO bronze"
    )
    parser.add_argument("--date", required=False, help="UTC yyyy-mm-dd", default=None)
    parser.add_argument("--hours", nargs="*", help="UTC hours e.g. 0 1 2", default=None)
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "gharchive"))
    parser.add_argument("--prefix", default="bronze")
    _args = parser.parse_args()

    # TODO: Challenge #2 작은 파일 재현을 위해 다수 시간대 샘플링 후 업로드
    # TODO: aiohttp/asyncio로 병렬 다운로드 및 메모리 업로드 구현
    raise NotImplementedError("ingest pipeline is not implemented yet")


if __name__ == "__main__":
    main()
