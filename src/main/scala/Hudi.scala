import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object Hudi {
  var path_data: String = _
  var path_hudi_table: String = _
  var conf: SparkConf = _
  var sc: SparkContext = _
  var spark: SparkSession = _
  var people_df: DataFrame = _
  var pwd: String = _
  var number_iterations = 5
  var hudi_options: Map[String, String] = _

  def time[T](f: => T, number_iterations: Int = 1): Unit = {
    val time_init = System.nanoTime
    for (i <- 1 to number_iterations) {
      var function_call = f
      function_call
    }
    val time_write = (System.nanoTime - time_init) / 1e9d
    println("Time per " + number_iterations + " iterations (s) : " + time_write)
    println("Time per  single iteration (s) : " + time_write / number_iterations)
  }


  def init_spark_session(): Unit = {
    pwd = System.getProperty("user.dir")
    path_data = pwd + "\\data\\brut_data\\data.parquet"
    path_hudi_table = pwd + "\\data\\hudi_database"
    conf = new SparkConf().setMaster("local").setAppName("Hudi_operation")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    people_df = spark.read.parquet(path_data)
    hudi_options = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> "people_table",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "id")
  }

  def display(): Unit = {
    people_df.show()
    println("Size df : " + people_df.count())
  }


  def insert(): Unit = {

    people_df = people_df.unionAll(people_df.limit(1))
    people_df.write.options(getQuickstartWriteConfigs)
      .options(hudi_options)
      .mode(SaveMode.Append)
      .save(path_hudi_table)

  }

  def write(): Unit = {
    people_df.write.
      options(getQuickstartWriteConfigs)
      .options(hudi_options)
      .mode(SaveMode.Overwrite)
      .save(path_hudi_table)
  }

  def update(): Unit = {
    people_df = people_df.withColumn("id", when(col("id") === 1, 1001)
      .otherwise(col("id")))
    people_df.write
      .options(getQuickstartWriteConfigs)
      .options(hudi_options)
      .mode(SaveMode.Append)
      .save(path_hudi_table)


  }

  def filter(): Unit = {
    /* Apply filter on Hudi table and save changes*/
    people_df.filter(col("first_name") === "Amanda").
      write.options(getQuickstartWriteConfigs)
      .option(OPERATION_OPT_KEY, "upsert")
      .options(hudi_options)
      .mode(SaveMode.Append)
      .save(path_hudi_table)

  }

  def delete(): Unit = {
    /* Apply delete + condition on Hudi table and save changes*/
    people_df = people_df.filter("id != 1")
    people_df.write.options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "delete")
      .options(hudi_options)
      .mode(SaveMode.Append)
      .save(path_hudi_table)

  }

  def calculate_time(): Unit = {
    println("+++++++++++++++++++++++++++++++ Writing +++++++++++++++++++++++++++++++")
    time(people_df.write.
      options(getQuickstartWriteConfigs)
      .options(hudi_options).mode(SaveMode.Overwrite)
      .save(path_hudi_table), number_iterations)
    println("+++++++++++++++++++++++++++++++ Updating +++++++++++++++++++++++++++++++")
    time({
      people_df.withColumn("id", when(col("id") === 1, 1001)
        .otherwise(col("id")))
      people_df.write
        .options(getQuickstartWriteConfigs)
        .options(hudi_options)
        .mode(SaveMode.Append)
        .save(path_hudi_table)
    }, number_iterations)
    println("+++++++++++++++++++++++++++++++ Inserting +++++++++++++++++++++++++++++++")
    time({
      people_df = people_df.unionAll(people_df.limit(1))
      people_df.write.options(getQuickstartWriteConfigs)
        .options(hudi_options)
        .mode(SaveMode.Append)
        .save(path_hudi_table)
    }, number_iterations)

    println("+++++++++++++++++++++++++++++++ Deleting  +++++++++++++++++++++++++++++++")
    time({
      people_df = people_df.filter("id != 1")
      people_df.write.options(getQuickstartWriteConfigs).
        option(OPERATION_OPT_KEY, "delete")
        .options(hudi_options)
        .mode(SaveMode.Append)
        .save(path_hudi_table)
    }, number_iterations)


  }

  def main(args: Array[String]): Unit = {
    init_spark_session()
    calculate_time()

  }
}

