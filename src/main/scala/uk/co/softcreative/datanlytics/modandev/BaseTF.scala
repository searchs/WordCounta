package uk.co.softcreative.datanlytics.modandev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BaseTF {
  /**
   *
   * Basic Spark operation leveraging the power of Scala
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Analytics example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.text("input/raw-coffee.txt")
    df.printSchema
    df.show(false)

    val converted = df.map { r =>
      r.getString(0).split(",") match {
        case Array(name: String, roast: String) => (name, roast.trim.toInt)
      }
    }.toDF("name", "roast")

    converted.show(false)
    converted.printSchema

    println("\n\tCSV READER: \n\t" + "==" * 45)
    val coffeeDF = spark.read.csv("input/raw-coffee.txt").toDF("name", "roast")
    coffeeDF.printSchema
    coffeeDF.show(false)

    // Using Spark config to infer Schema
    val coffeeAndSchemaDF = spark.read
      .option("inferSchema", true)
      .csv("input/raw-coffee.txt")
      .toDF("name", "roast")
    coffeeAndSchemaDF.printSchema

    // Specifying custom Schema
    import org.apache.spark.sql.types._

    val coffeeSchema: StructType = coffeeAndSchemaDF.schema
    val coffeeSchemaDDL: String = coffeeSchema.toDDL

    //    Create explicit schema (from stolen schema)
    val coffeeDDLStruct: StructType = StructType.fromDDL(coffeeSchemaDDL)

    // Read coffee csv with designed schema
    val coffees = spark.read
      .option("inferSchema", false)
      .schema(coffeeDDLStruct)
      .csv("input/raw-coffee.txt")

    coffees.printSchema

    val solidCoffeeSchema = StructType(
      Seq(
        StructField("name", StringType, metadata = new MetadataBuilder().putString("comment", "Coffee Brand Name").build()),
        StructField("roast", DoubleType, metadata = new MetadataBuilder().putString("comment", "Coffee Roast Level (1-10)").build())
      )
    )

    val coffeeData = spark.read
      .option("inferSchema", false)
      .schema(solidCoffeeSchema)
      .csv("input/raw-coffee.txt")

    println("\n\tSCHEMA - from defined Schema :D!\n")
    coffeeData.printSchema
    coffeeData.show(false)

    println("\n\tGoing the way of Spark SQL\n")




  }

}
