object Iceberg {
    import org.apache.spark.SparkContext
    import org.apache.spark.sql.SparkSession

    def main(args: Array[String]): Unit = {
      import org.apache.spark.SparkConf
      val conf = new SparkConf().setAppName("ICEBERG").setMaster("local[2]").set("spark.executor.memory", "1g")
      //val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val spark = SparkSession
        .builder()
        .getOrCreate();

      val addressDf = spark.read.format("iceberg").text("C:\\AGE-Lab\\Problem-Statement-1\\iceberg-table\\addresses.csv")
      addressDf.createOrReplaceTempView("address")
      val results = sqlContext.sql("SELECT * FROM address")
      //results.collect.foreach(println)
      results.show()
    }
  }
