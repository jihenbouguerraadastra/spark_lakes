import time
from delta.tables import *
from pyspark.sql import SparkSession
from pathlib import Path
import timeit

number_iteration = 10


class Delta_lake:

    def __init__(self, display):
        self.spark = SparkSession.builder.appName("Delta_crud") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.pwd = str(Path(__file__).parents[3])
        self.path_delta_table = self.pwd + "\\data\\delta_database"
        self.path_data = self.pwd + "\\data\\brut_data\\data.parquet"
        self.delta_table = None
        self.df = self.spark.read.load(self.path_data)
        self.display_ = display

    def read(self):
        self.delta_table = DeltaTable.forPath(self.spark, self.path_delta_table)

    def write(self):
        self.df.write.format("delta").mode("overwrite") \
            .save(self.path_delta_table)
        self.read()

    def insert(self):
        df_update = self.df.limit(1)
        self.delta_table.alias("people").merge(
            df_update.alias("updates"),
            "people.id = updates.id") \
            .whenMatchedUpdate(set={"id": "updates.id"}) \
            .whenNotMatchedInsert(values=
                                  {"registration_dttm": "updates.registration_dttm",
                                   "id": "updates.id",
                                   "first_name": "updates.first_name",
                                   "last_name": "updates.last_name",
                                   "email": "updates.email",
                                   "gender": "updates.gender",
                                   "ip_address": "updates.ip_address",
                                   "cc": "updates.cc",
                                   "country": "updates.country",
                                   "birthdate": "updates.birthdate",
                                   "salary": "updates.salary",
                                   "title": "updates.title",
                                   "comments": "updates.comments"

                                   }).execute()

        if self.display_:
            print("+++++++++++++++++++++++++++++++++++++++++++ Insert +++++++++++++++++++++++++++++++++++++++++++")
            self.display()

    def update(self):
        self.delta_table.update(set={"id": "id + 1000"})
        if self.display_:
            print("+++++++++++++++++++++++++++++++++++++++++++ Update +++++++++++++++++++++++++++++++++++++++++++")
            self.display()

        # self.delta_table.toDF().show()

    def delete(self):
        self.delta_table.delete(condition="id == 1")
        if self.display_:
            print("+++++++++++++++++++++++++++++++++++++++++++ Delete +++++++++++++++++++++++++++++++++++++++++++")
            self.display()

    def roll_back(self):
        self.df = self.spark.read.format("delta").option("versionAsOf", 0).load(self.path_delta_table)
        if self.display_:
            print("+++++++++++++++++++++++++++++++++++++++++++ Roll_back +++++++++++++++++++++++++++++++++++++++++++")
            self.display()

    def display(self):
        self.delta_table.toDF().show()
        print("***** Size Table:", self.delta_table.toDF().count())

    def __await__(self):
        time.sleep(300)


def calculate_time():
    delta_lake = Delta_lake(False)
    print(number_iteration, " writing operations")
    time_write = timeit.timeit(lambda: delta_lake.write(), number=number_iteration)
    print(number_iteration, " updating operations")
    time_update = timeit.timeit(lambda: delta_lake.update(), number=number_iteration)
    print(number_iteration, " inserting operations")
    time_insert = timeit.timeit(lambda: delta_lake.insert(), number=number_iteration)
    print(number_iteration, " deleting operations")
    time_delete = timeit.timeit(lambda: delta_lake.delete(), number=number_iteration)

    print(".............................................................................................")
    print("** Time for ", number_iteration, " writing operation ", time_write)
    print("** Time for a writing operation : ", time_write / number_iteration)
    print("** Time for ", number_iteration, " updating operation ", time_update)
    print("** Time for  an updating operation : ", time_update / number_iteration)
    print("** Time for ", number_iteration, " inserting operation ", time_insert)
    print("** Time for an inserting operation : ", time_insert / number_iteration)
    print("** Time for ", number_iteration, " deleting operations ", time_delete)
    print("** Time for a deleting operation : ", time_delete / number_iteration)


def test():
    delta_lake = Delta_lake(True)
    delta_lake.write()
    delta_lake.update()
    delta_lake.insert()
    delta_lake.delete()


if __name__ == '__main__':
    calculate_time()
