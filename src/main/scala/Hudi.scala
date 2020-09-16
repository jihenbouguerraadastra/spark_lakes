import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object Hudi {
  val path_data = "\\Users\\Jihen.Bouguerra" +
    "\\Documents\\GitHub\\spark_lakes\\src\\data\\brut_data\\data.parquet"
  val path_hudi_table = "\\Users\\Jihen.Bouguerra\\" +
    "Documents\\GitHub\\spark_lakes\\src\\data\\hudi"

  var conf: SparkConf = _
  var sc: SparkContext = _
  var spark: SparkSession = _
  var people_df: DataFrame = _

  def init_spark_session(): Unit = {
    conf = new SparkConf().
      setMaster("local").
      setAppName("Hello_world")
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
    people_df.filter(col("first_name") === "Amanda").
      write.options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, "people_table_after_delete").
      mode(SaveMode.Append).
      save(path_hudi_table)
  }


  def main(args: Array[String]): Unit = {
    init_spark_session()
    read_df()
    write_df()
    delete_df()

    
  }
}

//    var filter_df = spark.read.
//      parquet(path_hudi_table + "\\part-00000-4f4ea92c-a928-4fa7-8dfa-96c9c06a5360-c000.snappy.parquet")
//
//  filter_df.show()