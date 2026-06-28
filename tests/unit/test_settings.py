import pytest
from pydantic import ValidationError

from us_accidents_etl.config.settings import ETLConfig, SparkConfig, get_settings


def test_severity_validator_rejects_zero():
    with pytest.raises(ValidationError):
        ETLConfig(input_path="gs://a/b", output_path="gs://c/d", min_severity=0)


def test_severity_validator_rejects_five():
    with pytest.raises(ValidationError):
        ETLConfig(input_path="gs://a/b", output_path="gs://c/d", min_severity=5)


def test_severity_validator_accepts_boundary_values():
    cfg_low = ETLConfig(input_path="gs://a/b", output_path="gs://c/d", min_severity=1)
    cfg_high = ETLConfig(input_path="gs://a/b", output_path="gs://c/d", min_severity=4)
    assert cfg_low.min_severity == 1
    assert cfg_high.min_severity == 4


def test_spark_config_defaults():
    cfg = SparkConfig()
    assert cfg.master == "local[*]"
    assert cfg.remote is None
    assert cfg.executor_cores == 2


def test_get_settings_raises_without_required_env(monkeypatch):
    for key in [
        "ETL__INPUT_PATH",
        "ETL__OUTPUT_PATH",
        "ETL_INPUT_PATH",
        "ETL_OUTPUT_PATH",
    ]:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(Exception):
        get_settings(_env_file=None)
