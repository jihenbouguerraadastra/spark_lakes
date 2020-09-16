import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Hudi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Hello_world")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._
    val data = Seq(1, 2, 3, 3, 4, 5, 6)
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF("id")
    df.show()
    val tableName = "id"
    val basePath = "\\hadoop\\hudi-table\\data.parquet"

    var df2 = spark.read.parquet(basePath)
    df2.show()

    val hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .load(basePath);

  }
}

