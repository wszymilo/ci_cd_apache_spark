import glob
import os

import pytest
from pyspark.sql import SparkSession

from us_accidents_etl.config.settings import ETLConfig
from us_accidents_etl.load.writer import (
    write_aggregations,
    write_dataset,
    write_filtered,
)


def has_parquet(path: str) -> bool:
    return bool(glob.glob(os.path.join(path, "*.parquet")))


@pytest.fixture
def etl_cfg(tmp_path) -> ETLConfig:
    return ETLConfig(
        input_path="gs://dummy/input",
        output_path=str(tmp_path / "output"),
    )


def test_write_dataset_creates_parquet_locally(spark: SparkSession, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    out = str(tmp_path / "out")
    write_dataset(df, out)
    assert has_parquet(out)


def test_write_dataset_creates_output_dir_if_missing(spark: SparkSession, tmp_path):
    df = spark.createDataFrame([(42,)], ["x"])
    out = str(tmp_path / "nested" / "dir")
    write_dataset(df, out)
    assert os.path.isdir(out)


def test_write_filtered_writes_to_correct_subpath(
    spark: SparkSession, etl_cfg: ETLConfig
):
    df = spark.createDataFrame([(3, "Rain")], ["Severity", "Weather_Condition"])
    write_filtered(df, etl_cfg)
    assert has_parquet(os.path.join(etl_cfg.output_path, "filtered"))


def test_write_aggregations_writes_all_five_dirs(
    spark: SparkSession, etl_cfg: ETLConfig
):
    df = spark.createDataFrame([(1,)], ["x"])
    write_aggregations(df, df, df, df, df, etl_cfg)
    agg_base = os.path.join(etl_cfg.output_path, "agg")
    for name in (
        "severity_stats",
        "state_stats",
        "city_stats",
        "weather_stats",
        "day_night_stats",
    ):
        assert has_parquet(os.path.join(agg_base, name)), f"missing {name}"
