
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object Delta_lake {
  var path_data: String = _
  var path_delta_table: String = _
  var conf: SparkConf = _
  var sc: SparkContext = _
  var spark: SparkSession = _
  var people_df: DataFrame = _
  var pwd: String = _
  var number_iterations = 5
  var delta_table: DeltaTable = _


  def time[T](f: => T, number_iterations: Int = 1): Unit = {
    val time_init = System.nanoTime
    for (i <- 1 to number_iterations) {
      var function_call = f
      function_call
    }
    val duration = (System.nanoTime - time_init) / 1e9d
    println("Time per " + number_iterations + " iterations (s) : " + duration)
    println("Time per  single iteration (s) : " + duration / number_iterations)
  }

  def init_spark_session(): Unit = {
    pwd = System.getProperty("user.dir")
    path_data = pwd + "\\data\\brut_data\\data.parquet"
    path_delta_table = pwd + "\\data\\delta_database"
    conf = new SparkConf().setMaster("local").setAppName("Delta_operation")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    people_df = spark.read.parquet(path_data)

  }

  def read(): Unit = {
    delta_table = DeltaTable.forPath(spark, path_delta_table)
  }

  def display(): Unit = {
    people_df.show()
    println("Size df : " + people_df.count())
  }


  def insert(): Unit = {
    var df_insert = people_df.limit(1)
    delta_table.alias("people").merge(df_insert.alias("updates"), "people.id = updates.id")
      .whenNotMatched
      .insertExpr(
        Map("registration_dttm" -> "updates.registration_dttm",
          "id" -> "updates.id",
          "first_name" -> "updates.first_name",
          "last_name" -> "updates.last_name",
          "email" -> "updates.email",
          "gender" -> "updates.gender",
          "ip_address" -> "updates.ip_address",
          "cc" -> "updates.cc",
          "country" -> "updates.country",
          "birthdate" -> "updates.birthdate",
          "salary" -> "updates.salary",
          "title" -> "updates.title",
          "comments" -> "updates.comments"
        ))
      .execute()


  }

  def write(): Unit = {
    people_df.write.format("delta").mode("overwrite")
      .save(path_delta_table)
  }

  def update(): Unit = {

    delta_table.updateExpr(
      Map("id" -> "id + 1000 "))
  }

  def delete(): Unit = {
    delta_table.delete(condition = "id == 1")
  }

  def calculate_time(): Unit = {
    read()
    println("+++++++++++++++++++++++++++++++ Writing +++++++++++++++++++++++++++++++")
    time(write(), number_iterations)
    println("+++++++++++++++++++++++++++++++ Updating +++++++++++++++++++++++++++++++")
    time(update(), number_iterations)
    println("+++++++++++++++++++++++++++++++ Inserting +++++++++++++++++++++++++++++++")
    time(insert(), number_iterations)
    println("+++++++++++++++++++++++++++++++ Deleting  +++++++++++++++++++++++++++++++")
    time(delete(), number_iterations)


  }

  def main(args: Array[String]): Unit = {
    init_spark_session()
    calculate_time()

  }
}
