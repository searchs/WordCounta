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
    coffeeData.createOrReplaceTempView("coffeeData")
    spark.sql("desc coffeeData").show(truncate = false)

    spark.sql("select * from coffeeData").show(false)

    //    Average roast calculations
    spark.sql("select avg(roast) as avg_roast from coffeeData").show(false)

    // Return Minimum roast
    spark.sql("select min(roast) as min_roast from coffeeData").show(false)
    // Return Maximum roast
    spark.sql("select max(roast) as max_roast from coffeeData").show(false)
    // Order by Roast value descending
    spark.sql("select * from coffeeData order by roast desc").show(false)
    // Order by Name - in ascending order
    spark.sql("select * from coffeeData order by name asc").show(false)

    //Simple ETL

    spark.read.option("inferSchema", "false")
      .schema(solidCoffeeSchema)
      .csv("input/raw-coffee.txt")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("input/coffee-data.parquet")

    //TODO:  Switch to logger - remove print statements!!!!!
    println("\t\nTest output to complete ETL process")
    spark.read.parquet("input/coffee-data.parquet").createOrReplaceTempView("coffeeResult")
    spark.sql("desc coffeeResult").show(false)

    println("ETL now complete!")

  }

}
