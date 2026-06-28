from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from us_accidents_etl.config.settings import SparkConfig
from us_accidents_etl.spark.session import create_spark_session


def test_create_spark_session_local_returns_spark_session_instance():
    cfg = SparkConfig(remote=None)
    spark = create_spark_session(cfg)
    assert isinstance(spark, SparkSession)


def test_create_spark_session_local_passes_master_to_builder():
    builder = MagicMock()
    with patch.object(SparkSession, "builder", new=builder):
        create_spark_session(SparkConfig(remote=None, master="local[4]"))

    builder.master.assert_called_once_with("local[4]")


def test_create_spark_session_local_passes_app_name_to_builder():
    builder = MagicMock()
    with patch.object(SparkSession, "builder", new=builder):
        create_spark_session(SparkConfig(remote=None, app_name="my-etl-app"))

    builder.master.return_value.appName.assert_called_once_with("my-etl-app")


def test_create_spark_session_local_passes_all_configs_through_chain():
    builder = MagicMock()
    expected = MagicMock(spec=SparkSession)
    chain = builder.master.return_value
    chain = chain.appName.return_value
    chain = chain.config.return_value
    chain = chain.config.return_value
    chain = chain.config.return_value
    chain.getOrCreate.return_value = expected

    cfg = SparkConfig(
        master="local[2]",
        app_name="unit-test",
        executor_memory="4g",
        driver_memory="2g",
        executor_cores=2,
        remote=None,
    )

    with patch.object(SparkSession, "builder", new=builder):
        result = create_spark_session(cfg)

    assert result is expected
    builder.master.assert_called_once_with("local[2]")
    builder.master.return_value.appName.assert_called_once_with("unit-test")

    c1 = builder.master.return_value.appName.return_value.config
    c2 = c1.return_value.config
    c3 = c2.return_value.config

    c1.assert_called_once_with("spark.executor.memory", "4g")
    c2.assert_called_once_with("spark.driver.memory", "2g")
    c3.assert_called_once_with("spark.executor.cores", "2")
    c3.return_value.getOrCreate.assert_called_once()


def test_create_spark_session_local_casts_executor_cores_to_string():
    builder = MagicMock()
    with patch.object(SparkSession, "builder", new=builder):
        create_spark_session(SparkConfig(remote=None, executor_cores=4))

    c3 = (
        builder.master.return_value.appName.return_value.config.return_value.config.return_value.config
    )
    c3.assert_called_once_with("spark.executor.cores", "4")


def test_create_spark_session_local_does_not_set_authenticate_secret():
    builder = MagicMock()
    with patch.object(SparkSession, "builder", new=builder):
        create_spark_session(SparkConfig(remote=None))

    config_keys = []
    node = builder.master.return_value.appName.return_value
    while hasattr(node, "config") and node.config.called:
        config_keys.append(node.config.call_args.args[0])
        node = node.config.return_value

    assert "spark.authenticate.secret" not in config_keys


def test_create_spark_session_remote_uses_remote_builder():
    expected = MagicMock(spec=SparkSession)
    builder = MagicMock()
    builder.remote.return_value.getOrCreate.return_value = expected

    with patch.object(SparkSession, "builder", new=builder):
        result = create_spark_session(SparkConfig(remote="sc://localhost:15002"))

    builder.remote.assert_called_once_with("sc://localhost:15002")
    assert result is expected


def test_create_spark_session_remote_skips_local_builder_chain():
    builder = MagicMock()
    builder.remote.return_value.getOrCreate.return_value = MagicMock(spec=SparkSession)

    with patch.object(SparkSession, "builder", new=builder):
        create_spark_session(
            SparkConfig(
                remote="sc://localhost:15002",
                app_name="should-be-ignored",
                executor_memory="999g",
            )
        )

    builder.master.assert_not_called()
    builder.appName.assert_not_called()
    builder.config.assert_not_called()
