import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Hudi {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().
      setMaster("local").
      setAppName("Hello_world")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    import spark.implicits._
    val columns = Seq("id")
    val data = Seq(1, 2, 3, 3, 4, 5, 6)
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD1 = rdd.toDF("id")
    dfFromRDD1.show()

  }
}

