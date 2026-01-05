"""
데이터 쏠림 재현/완화 유틸
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F


def inflate_skew(*, df: DataFrame, repo_name: str, multiplier: int) -> DataFrame:
    if multiplier <= 1:
        return df
    logging.info("쏠림 재현: repo=%s multiplier=%d", repo_name, multiplier)
    skew_df = df.filter(F.col("repo_name") == F.lit(repo_name))
    rest_df = df.filter(F.col("repo_name") != F.lit(repo_name))
    dup_df = skew_df.withColumn(
        "dup", F.explode(F.sequence(F.lit(1), F.lit(multiplier)))
    ).drop("dup")
    return rest_df.unionByName(dup_df)


def pick_skewed_repos(repo_counts: DataFrame, top_k: int) -> list[str]:
    return [row["repo_name"] for row in repo_counts.limit(top_k).collect()]


def resolve_skew_target(
    *,
    repo_counts: DataFrame,
    preferred_repo: str | None,
) -> str | None:
    if preferred_repo:
        matched = (
            repo_counts.filter(F.col("repo_name") == F.lit(preferred_repo))
            .limit(1)
            .collect()
        )
        if matched:
            return preferred_repo
        logging.warning(
            "지정 repo를 찾지 못해 상위 레포로 대체합니다: %s", preferred_repo
        )
    top_rows = repo_counts.limit(1).collect()
    if not top_rows:
        return None
    return top_rows[0]["repo_name"]


def resolve_skewed_repos(
    *,
    repo_counts: DataFrame,
    top_k: int,
    preferred_repo: str | None,
) -> list[str]:
    if preferred_repo:
        matched = (
            repo_counts.filter(F.col("repo_name") == F.lit(preferred_repo))
            .limit(1)
            .collect()
        )
        if matched:
            return [preferred_repo]
        logging.warning(
            "지정 repo를 찾지 못해 상위 레포로 대체합니다: %s", preferred_repo
        )
    return pick_skewed_repos(repo_counts, top_k)


def apply_salting(
    *,
    df: DataFrame,
    skewed_repos: list[str],
    salt_buckets: int,
    salt_seed: int,
) -> DataFrame:
    if not skewed_repos:
        return df.withColumn("salt", F.lit(0))
    return df.withColumn(
        "salt",
        F.when(
            F.col("repo_name").isin(skewed_repos),
            F.floor(F.rand(seed=salt_seed) * F.lit(salt_buckets)),
        ).otherwise(F.lit(0)),
    )


def salted_repo_counts(
    *,
    df: DataFrame,
    skewed_repos: list[str],
    salt_buckets: int,
    salt_seed: int,
) -> DataFrame:
    salted = apply_salting(
        df=df,
        skewed_repos=skewed_repos,
        salt_buckets=salt_buckets,
        salt_seed=salt_seed,
    )
    partial = salted.groupBy("repo_name", "salt").count()
    return (
        partial.groupBy("repo_name")
        .agg(F.sum("count").alias("count"))
        .orderBy(F.desc("count"))
    )
