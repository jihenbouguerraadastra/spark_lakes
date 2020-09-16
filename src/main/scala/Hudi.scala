import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor

object Hudi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Hello_world")
    var path_data = "\\Users\\Jihen.Bouguerra" +
      "\\Documents\\GitHub\\spark_hudi\\src\\data\\brut_data\\data.parquet"
    var path_hudi_table = "\\Users\\Jihen.Bouguerra\\" +
      "Documents\\GitHub\\spark_hudi\\src\\data\\hudi"


    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder
      .config(sc.getConf)
      .getOrCreate()
    import spark.implicits._
    val data = Seq(1, 2, 3, 3, 4, 5, 6)
    val id_df = spark.sparkContext.parallelize(data).toDF()
    id_df.show()

    var people_df = spark.read.parquet(path_data)
    people_df.show()
    val hudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> "people_table",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "id"
    )

    people_df.write.
      options(getQuickstartWriteConfigs).
      options(hudiOptions)
      .mode(SaveMode.Overwrite).
      save(path_hudi_table)

    var rest = people_df.filter(col("first_name") === "Amanda").
      write.options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "upsert").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, "people_table_filtred").
      mode(SaveMode.Append).
      save(path_hudi_table)

        val hudi_table = spark
          .read
          .format("org.apache.hudi")
          .load(s"$path_hudi_table/*")

        hudi_table.show(truncate = false)

//    var filter_df = spark.read.
//      parquet(path_hudi_table + "\\part-00000-4f4ea92c-a928-4fa7-8dfa-96c9c06a5360-c000.snappy.parquet")
//
//  filter_df.show()
  }
}

