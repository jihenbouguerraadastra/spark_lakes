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

  def init_spark_session(): Unit = {

    pwd = System.getProperty("user.dir")
    path_data = pwd + "\\data\\brut_data\\data.parquet"
    path_hudi_table = pwd + "\\data\\hudi_database"

    conf = new SparkConf().
      setMaster("local").
      setAppName("Hudi_operation")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder
      .config(sc.getConf)
      .getOrCreate()
  }

  def read_df(): Unit = {
    people_df = spark.read.parquet(path_data)
    people_df.show()

    spark.time(people_df.show())
  }

  def write_df(): Unit = {
    val hudi_options = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> "people_table",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "id"
    )
    people_df.write.
      options(getQuickstartWriteConfigs).
      options(hudi_options)
      .mode(SaveMode.Overwrite).
      save(path_hudi_table)

    spark.time(people_df.show())
  }

  def update_df(): Unit = {
    val hudi_options = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> "people_table",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "id"
    )
    people_df.write.
      options(getQuickstartWriteConfigs).
      options(hudi_options)
      .mode(SaveMode.Append).
      save(path_hudi_table)

    spark.time(people_df.show())
  }

  def filter_df(): Unit = {
    /* Apply filter on Hudi table and save changes*/
    people_df.filter(col("first_name") === "Amanda").
      write.options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "upsert").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, "people_table_filtered").
      mode(SaveMode.Append).
      save(path_hudi_table)

  }

  def delete_df(): Unit = {
    /* Apply delete + condition on Hudi table and save changes*/
    people_df.write.options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, "people_table_after_delete").
      mode(SaveMode.Append).
      save(path_hudi_table)

    spark.time(people_df.show())
  }


  def main(args: Array[String]): Unit = {
    init_spark_session()
    read_df()
    write_df()
    update_df()
    //filter_df()
    delete_df()
  }
}

