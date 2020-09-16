import time

from delta.tables import *
from pyspark.sql import SparkSession


class Delta_lake:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Delta_crud") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.data_path = "C:\\Users\\Jihen.Bouguerra\\hadoop\\delta-table"
        self.df = None
        self.delta_table = None

    def read_df(self):
        self.df = self.spark.read.format("delta") \
            .load(self.data_path)
        self.df.show()

    def init_df(self):
        data = self.spark.range(0, 5)
        data.write.format("delta").mode("overwrite") \
            .save(self.data_path)

    def update_df(self):
        self.delta_table = DeltaTable.forPath(self.spark, self.data_path)
        self.delta_table.update(condition="id % 2 == 0",
                                set={"id": "id + 100"})
        self.delta_table.toDF().show()

    def delete_df(self):
        self.delta_table.delete(condition="id % 5 == 2")
        self.delta_table.toDF().show()

    def roll_back_df(self):
        self.df = self.spark.read.format("delta").option("versionAsOf", 0).load(self.data_path)
        self.df.show()

    def __await__(self):
        time.sleep(300)


if __name__ == '__main__':
    delta_lake = Delta_lake()
    delta_lake.init_df()
    delta_lake.read_df()
    delta_lake.update_df()
    delta_lake.delete_df()
    delta_lake.roll_back_df()
    delta_lake.__await__()
