import org.apache.spark.{SparkConf, SparkContext}

object Hello_world {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Hello_world")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val helloWorldString = "Hello World!"
    print(helloWorldString)

  }
}
