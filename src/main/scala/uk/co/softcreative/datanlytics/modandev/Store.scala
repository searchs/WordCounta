package uk.co.softcreative.datanlytics.modandev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object StoreOperations {
  case class StoreOccupants(storename: String, occupants: Int)

  case class Store(name: String, capacity: Int, opens: Int, closes: Int)

  private val occupants = Seq(
    StoreOccupants("a", 8),
    StoreOccupants("b", 20),
    StoreOccupants("c", 16),
    StoreOccupants("d", 55),
    StoreOccupants("e", 8),
    StoreOccupants("f", 23)
  )

  private val stores = Seq(
    Store("a", 24, 8, 10),
    Store("b", 36, 7, 21),
    Store("c", 18, 5, 23),
    Store("d", 42, 8, 18)
  )


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Analytics")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val occupancy = spark.createDataFrame(occupants)
    occupancy.createOrReplaceTempView("store_occupants")

    val df = spark.createDataFrame(stores)
    df.createOrReplaceTempView("stores")
    spark.sql("select * from stores").show(false)
    spark.sql("select * from stores where closes >= 22 ").show(false)

    //    Filtering with WHERE
    df.where(df("closes") >= 22).show(false)
    df.where(col("closes") >= 22).show(false)
    df.where('closes >= 22).show(false)
    df.where($"closes" >= 22).show(false)

    //  Filtering with FILTER
    df.filter($"closes" >= 22).show(false)

    //    Data Projection
    spark.sql("select name from stores where capacity > 21").show(false)

    //  via DataFrame API
    df.select("name").where('capacity > 20).show(false)

    //JOINS

    // Inner Join
    //1. Spark SQL
    spark.sql(
      """select * from stores
        inner join store_occupants
         on stores.`name` == store_occupants.`storename`""".stripMargin).show(false)

    //2.  DataFrame
    df.join(occupancy).where(df("name") === occupancy("storename")).show(false)

    // Right Join
    println("\n\tRIGHT JOINS - SPARK SQL\n")
    // 1.
spark.sql(
  """
    select stores.*, store_occupants.`occupants` from stores
    right join store_occupants on stores.`name` == store_occupants.`storename`
    """.stripMargin).show(false)
//    WHERE stores.`capacity` < 36 AND store_occupants.`occupants` < 15
    // 2. DataFrame
    println("\n\tRIGHT JOINS - SPARK DATAFRAME\n")

    df.join(occupancy, df("name") === occupancy("storename"), "right").show(false)


    // Left Join
    println("\n\tLEFT JOINS - SPARK SQL\n")
    // 1.
    spark.sql(
      """
    select stores.*, store_occupants.`occupants` from stores
    left join store_occupants on stores.`name` == store_occupants.`storename`
    """.stripMargin).show(false)
    // 2. DataFrame
    println("\n\tLEFT JOINS - SPARK DATAFRAME\n")

    df.join(occupancy, df("name") === occupancy("storename"), "left").show(false)

  }
}