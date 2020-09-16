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
    var path_data = "C:\\Users\\Jihen.Bouguerra" +
      "\\Documents\\GitHub\\spark_hudi\\src\\data\\brut_data\\data.parquet"
    var path_hudi_table = "C:\\Users\\Jihen.Bouguerra\\" +
      "Documents\\GitHub\\spark_hudi\\src\\data\\hudi"


    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder
      .config(sc.getConf)
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
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


    val hudi_table = spark
      .read
      .format("org.apache.hudi")
      .load(s"$path_hudi_table").show(truncate = false)


  }
}

