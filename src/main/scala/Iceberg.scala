import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Iceberg {

  import org.apache.spark.SparkConf

  val conf = new SparkConf().setAppName("ICEBERG").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val spark = SparkSession
    .builder()
    .getOrCreate();

  val data_path = "C:\\AGE-Lab\\Problem-Statement-1\\iceberg-table\\addresses.csv"
  val addressDf = spark.read.option("inferSchema", "true").format("iceberg").text(data_path)
  addressDf.createOrReplaceTempView("address")
  //val results = sqlContext.sql("SELECT * FROM address")
  //results.show()

  def read_df(): Unit = {
    val addressDf = spark.read
      .option("inferSchema", "true")
      .format("iceberg")
      .text(data_path)
    addressDf.show()
  }

  def write_df(): Unit = {
    addressDf.write
      .mode("append")
      .save("C:\\AGE-Lab\\Problem-Statement-1\\spark_lakes\\iceberg-tables")
  }

  def filter_df(): Unit = {
    spark
      .sql("select * from address where value LIKE 'John%'")
      .show()
  }

  //Row-level deletes are not supported in the current format version
  def delete_df(): Unit = {
    val add = spark
      .sql("select * from address where value LIKE 'John%'")
      .drop("value")
  }

  def main(args: Array[String]): Unit = {
    read_df()
    write_df()
    filter_df()
    delete_df()
  }
}