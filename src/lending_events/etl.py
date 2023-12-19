import os
import pytz
import shutil
import logging
from typing import Literal

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    import sys
    sys.path.append(os.path.abspath(os.path.join(__file__, "..", "..")))
from lending_events.crawler import QueryLendingEvents


TIME_ZONE = pytz.timezone("Asia/Ho_Chi_Minh")
logger = logging.getLogger("LendingEvents")


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Python Spark SQL basic example")
            .master("local[*]")
            .config("spark.executor.memory", "4G")
            .config("spark.driver.memory","18G")
            .config("spark.executor.cores","4")
            .config("spark.python.worker.memory","4G")
            .config("spark.driver.maxResultSize","6G")
            .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max","1024M")
            .getOrCreate()
    )


class LendingEvents_ETL_Processor:
    def __init__(
            self, 
            spark: SparkSession, 
            input_path: str, 
            output_dir: str, 
            event_type: str, 
            save_mode: Literal["append", "overwrite", "auto"]="overwrite",
            final: bool=False
    ) -> None:
        self.spark = spark
        self.event_type = event_type
        self.user_col_name = QueryLendingEvents.get_user_column_name(event_type)
        self.output_dir = output_dir
        self.save_mode = save_mode
        self.final = final

        if save_mode == "overwrite":
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        self.df = self.read(input_path)
        logger.info(f"Reading data from {input_path}")

    def read(self, data_path: str):
        spark = self.spark
        if os.path.exists(data_path) is False:
            raise ValueError(f"Path '{data_path}' not found")
        if os.path.isdir(data_path):
            return spark.read.option("mergeSchema", "true").parquet(data_path)
        if os.path.isfile(data_path):
            if data_path.endswith(".csv"):
                return spark.read.option("header", True).csv(data_path)
            if data_path.endswith(".parquet"):
                return spark.read.option("mergeSchema", "true").parquet(data_path)
        raise ValueError(f"Cannot load data from '{data_path}'")
    
    def write(self, df, to: Literal["contract_interaction", "volume"], mode: str=None):
        output_dir = os.path.join(self.output_dir, to)
        mode = self.save_mode if mode is None else mode
        if mode == "auto":
            if os.path.exists(output_dir):
                mode = "append"
            else:
                mode = "overwrite"
        df.write.mode(mode).parquet(output_dir)

    def run(self):
        self.user_interact_with_contract_address()
        self.user_interact_timestamp()
        self.process_volume()
        if self.final:
            logger.info("Finalizing ...")
            self.finalize()

    def user_interact_with_contract_address(self):
        dc = self.df.groupBy(F.col(self.user_col_name).alias("user"), "contract_address").count()
        self.write(dc, "contract_interaction")

    def user_interact_timestamp(self):
        dt = self.df.select(F.col(self.user_col_name).alias("user"), "block_timestamp")
        self.write(dt, "timestamp")

    def finalize(self):
        output_dir = os.path.join(self.output_dir, "contract_interaction")
        df = self.read(output_dir)
        dc = df.groupBy("user", "contract_address").agg(F.sum("count").alias("count"))
        self.write(dc, "temp", "overwrite")
        shutil.rmtree(output_dir)
        os.rename(os.path.join(self.output_dir, "temp"), output_dir)

    def process_volume(self):
        func_name = self.event_type.lower() + "_volume"
        if hasattr(self, func_name) is False:
            raise ValueError(f"Event type {self.event_type} is not supported")
        getattr(self, func_name)()

    def _volume(self, df, reserve_col_name: str, amount_col_name: str, volume_type: str):
        dr = (
            df.groupBy(
                F.col(self.user_col_name).alias("user"), 
                "event_type",
                F.col(reserve_col_name).alias("reserve"),
            )
            .agg(F.sum(F.col(amount_col_name)).alias(f"total_amount"))
            .withColumn("volume_type", F.lit(volume_type))
        )
        self.write(dr, "volume")

    def deposit_volume(self):
        assert self.event_type.lower() == "deposit"
        self._volume(self.df, "reserve", "amount", "sending")

    def borrow_volume(self):
        assert self.event_type.lower() == "borrow"
        self._volume(self.df, "reserve", "amount", "receiving")

    def repay_volume(self):
        assert self.event_type.lower() == "repay"
        self._volume(self.df, "reserve", "amount", "receiving")

    def withdraw_volume(self):
        assert self.event_type.lower() == "withdraw"
        dr = self.df.withColumn("reserve", F.when(F.col("to").isNull(), F.col("contract_address")).otherwise(F.col("reserve")))
        self._volume(dr, "reserve", "amount", "receiving")
    
    def liquidate_volume(self):
        assert self.event_type.lower() == "liquidate"
        self._volume(self.df, "debt_asset", "debt_to_cover", "sending")
        self._volume(self.df, "collateral_asset", "liquidated_collateral_amount", "receiving")


if __name__ == "__main__":    
    from utils.logger import setup_logging
    setup_logging("outputs/logs/etl_processing")
    
    spark = get_spark_session()

    raw_data_dir = "data/lending_events/raw"
    output_dir = "data/lending_events/elite"

    data_fns = [fn for fn in os.listdir(raw_data_dir) if fn.endswith(".csv") and "flashloan" not in fn]
    for idx, fn in enumerate(data_fns):
        LendingEvents_ETL_Processor(
            spark,
            input_path=os.path.join(raw_data_dir, fn),
            output_dir=output_dir,
            event_type=os.path.splitext(fn)[0],
            save_mode="append" if idx > 0 else "overwrite",
            final=True if idx == len(data_fns) - 1 else False
        ).run()

    spark.stop()


# -- To group table timestamp by user, use:
# -- dt = df.groupBy("user").agg(F.collect_list("block_timestamp"))