package uk.co.softcreative.datanlytics
package uk.co.softcreative.datanlytics.procs

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object WordCounta extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream Word Count")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    linesDF.printSchema()
    val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word"))
    wordsDF.printSchema()

    val countDF = wordsDF.groupBy("word").count()
    countDF.printSchema()

    val wordCountQuery = countDF.writeStream
      .format("console")
      .option("checkpointLocation", "checkpointdir")
      .outputMode("complete")
      .start()

    logger.info("Listening to localhost on port 9999...")
  wordCountQuery.awaitTermination()



  }


}
