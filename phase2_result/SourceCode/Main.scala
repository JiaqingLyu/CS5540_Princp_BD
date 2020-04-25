import org.apache.spark.sql.SparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.format("csv").option("header","true").load("/Users/louis_lyu/Desktop/phase2/tweets.csv")
    df.createOrReplaceTempView("tmp")
    df.show()

//    val df1 = spark.sql("select Count(id) from tmp where text like '%Trump%'")
//    df1.show()
//    println("These are the tweets mentioned Trump")
//
//    val df2 = spark.sql("select Count(id) from tmp where text like '%mask%' or text like '%gloves%'")
//    df2.show()
//    println("These are the tweets mentioned mask or gloves")

//    val df3 = spark.sql("select Count(id) from tmp")
//    df3.show()
//    println("Total tweets")
//    val df4 = spark.sql("select Count(id) from tmp where text like '%stay at home%' or text like '%stay%'")
//    df4.show()
//    println("Tweets related to stay at home")
//    val df5 = spark.sql("select Count(id) from tmp where text like '%back%' and text like '%work%'")
//    df5.show()
//    println("Tweets related to back to work")
//        val df6 = spark.sql("select Count(id) from tmp where text like '%end%' and text like '%lock%'")
//        df6.show()
//        println("Tweets related to end the lock down")
    val df7 = spark.sql("select Count(id) from tmp where text like '%death%' or text like'%cases%' or text like'%increase%'")
    df7.show()
    println("Tweets related to cases and death")
    //print("Write to file")
    //df.write.parquet("/Users/louis_lyu/Desktop/ICP2_3/survey")
    
  }
}
