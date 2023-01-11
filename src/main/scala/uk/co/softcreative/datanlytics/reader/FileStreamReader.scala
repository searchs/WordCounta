package uk.co.softcreative.datanlytics
package uk.co.softcreative.datanlytics.reader

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object FileStreamReader {

  /**
   * Algorithm for Spark Streaming
   * - Initialize Spark session with appropriate config/options
   * - Ingest data from source
   * - Transform data based on business rules
   * - Write output to desired location e.g. directory, S3 or Google bucket
   * - Exit strategy - awaitTermination
   */
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream File contents")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "input")
      .option("maxFilesPerTrigger",1)
      .load()

    //    rawDF.printSchema()

    val explodeDF = rawDF.selectExpr("InvoiceNumber",
      "CreatedTime",
      "StoreID",
      "PosID",
      "CustomerType",
      "PaymentMethod",
      "DeliveryType",
      "DeliveryAddress.City",
      "DeliveryAddress.State",
      "DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem"
    )

    //  explodeDF.printSchema()

    val flattenDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    flattenDF.printSchema()

    val invoiceWriterQuery = flattenDF.writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointLocation", "inv-chk-dir")
      .outputMode("append")
      .queryName("Flatten Invoice Writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("\n\tInvoice FLATTENING  - write in progress\n ")
    invoiceWriterQuery.awaitTermination()
  }

}
