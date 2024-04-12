# Standard libs
from abc import ABC, abstractmethod
from logging import Logger
from pyspark.sql import SparkSession
from typing import Any, Dict, Optional


class DatabricksUtils:
    def __init__(
        self,
        init_conf: Optional[Dict] = None,
        app_name: str = "airlines_analyzer",
        spark=None,
        data: Any = None,
        spark_log_level: str = 'WARN',
        log_level: str = "DEBUG"
    ):
        self.app_name = app_name
        self.spark = self._prepare_spark(spark, app_name, spark_log_level)
        self.logger = self._prepare_logger(log_level)
        self.dbutils = self.get_dbutils()
        self.conf = self.init_config(init_conf)
        self.data = data

    def _prepare_logger(self, log_level: str) -> Logger:
        log_level = log_level.upper()

        log4j_logger = self.spark._jvm.org.apache.log4j
        _log4j_logger = log4j_logger.LogManager.getLogger(self.__class__.__name__)

        if log_level == "DEBUG":
            _log4j_logger.setLevel(log4j_logger.Level.DEBUG)
        elif log_level == "INFO":
            _log4j_logger.setLevel(log4j_logger.Level.INFO)
        elif log_level == "WARN":
            _log4j_logger.setLevel(log4j_logger.Level.WARN)
        elif log_level == "ERROR":
            _log4j_logger.setLevel(log4j_logger.Level.ERROR)

        return _log4j_logger

    @staticmethod
    def _prepare_spark(
        spark: SparkSession,
        app_name,
        spark_log_level
    ) -> SparkSession:
        if not spark:
            spark_session = SparkSession.Builder().appName(app_name).getOrCreate()
            spark_session.sparkContext.setLogLevel(spark_log_level)
            return spark_session
        else:
            return spark
