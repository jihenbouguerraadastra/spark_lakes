import time
from delta.tables import *
from pyspark.sql import SparkSession
from pathlib import Path


class Delta_lake:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Delta_crud") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.pwd = str(Path(__file__).parents[3])
        self.path_delta_table = self.pwd + "\\data\\delta_database"
        self.df = None
        self.path_data = self.pwd + "\\data\\brut_data\\data.parquet"
        self.delta_table = None

    def read_df(self):
        print()
        self.df = self.spark.read.load(self.path_data)
        self.df.show()

    def write_df(self):
        self.df.write.format("delta").mode("overwrite") \
            .save(self.path_delta_table)

    def update_df(self):
        self.delta_table = DeltaTable.forPath(self.spark, self.path_delta_table)
        self.delta_table.update(condition="id % 2 == 0",
                                set={"cc": "cc + 100"})
        self.delta_table.toDF().show()

    def delete_df(self):
        self.delta_table.delete(condition="id  == 1")
        self.delta_table.toDF().show()

    def roll_back_df(self):
        self.df = self.spark.read.format("delta").option("versionAsOf", 0).load(self.path_delta_table)
        self.df.show()

    def __await__(self):
        time.sleep(300)


if __name__ == '__main__':
    delta_lake = Delta_lake()
    delta_lake.read_df()
    delta_lake.write_df()
    delta_lake.update_df()
    delta_lake.delete_df()
    delta_lake.roll_back_df()
