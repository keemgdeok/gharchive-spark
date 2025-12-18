"""
Spark 파일시스템 유틸 (Hadoop FileSystem)
- S3A 등 scheme 기반 Path 처리용
"""

from __future__ import annotations


def get_fs_for_path(spark, path: str):
    jvm = spark._jvm  # pyright: ignore[reportAttributeAccessIssue]
    jsc = spark.sparkContext._jsc  # pyright: ignore[reportAttributeAccessIssue]
    hconf = jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(hconf), jpath


def path_exists(spark, path: str) -> bool:
    fs, jpath = get_fs_for_path(spark, path)
    return fs.exists(jpath)


def list_files_under(spark, dir_path: str) -> list[tuple[str, int]]:
    fs, jpath = get_fs_for_path(spark, dir_path)
    try:
        statuses = fs.listStatus(jpath)
    except Exception:  # noqa: BLE001
        return []

    results: list[tuple[str, int]] = []
    for st in statuses:
        if st.isFile():
            results.append((st.getPath().toString(), int(st.getLen())))
    return results
