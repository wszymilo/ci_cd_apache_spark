import logging

from pyspark.sql import SparkSession

from src.us_accidents_etl.config.settings import Settings
from src.us_accidents_etl.extract.reader import read_accidents_csv
from src.us_accidents_etl.load.writer import write_aggregations, write_filtered
from src.us_accidents_etl.transform import aggregations as agg
from src.us_accidents_etl.transform.enrichments import enrich
from src.us_accidents_etl.transform.filters import apply_etl_filters

logger = logging.getLogger(__name__)


def run(spark: SparkSession, settings: Settings) -> None:
    logger.info("Starting US Accidents ETL")

    # ── Extract ────────────────────────────────────────────────────────────────
    logger.info("Reading: %s", settings.etl.input_path)
    raw_df = read_accidents_csv(spark, settings.etl)

    # ── Transform ──────────────────────────────────────────────────────────────
    logger.info(
        "Filtering: severity >= %d, weather in %s",
        settings.etl.min_severity,
        settings.etl.weather_conditions,
    )
    filtered_df = apply_etl_filters(raw_df, settings.etl)
    enriched_df = enrich(filtered_df)

    severity = agg.severity_stats(raw_df)
    states = agg.state_stats(raw_df)
    cities = agg.city_stats(raw_df)
    weather = agg.weather_stats(raw_df)
    day_night = agg.day_night_stats(raw_df)

    # ── Load ───────────────────────────────────────────────────────────────────
    logger.info("Writing output to: %s", settings.etl.output_path)
    write_filtered(enriched_df, settings.etl)
    write_aggregations(severity, states, cities, weather, day_night, settings.etl)
