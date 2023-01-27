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
  )

  private val stores = Seq(
    Store("a", 24, 8, 10),
    Store("b", 36, 7, 21),
    Store("c", 18, 5, 23),
    Store("d", 42, 8, 18),
    Store("z", 28, 6, 17)

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

    // SEMI-JOIN: Uses other dataset as a criteria to filter main dataset e.g. Usecase: give me all the items from my store that matches criteria from a competitor

//    SPARK SQL
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW boutiques as  (SELECT stores.`name` as boutiquename FROM stores WHERE capacity < 20) """)
    spark.sql("""SELECT * FROM stores semi join boutiques on stores.`name` ==  boutiques.`boutiquename` """)


//    SPARK DATAFRAME
    val boutiqueDF = spark.sql("SELECT * FROM boutiques")
    val semiJoin = df.join(boutiqueDF,
      df("name") === boutiqueDF("boutiquename"), "semi")

    semiJoin.show(false)

//    ANTI JOIN or LEFT ANTI-JOIN
    spark.sql("""SELECT * FROM stores anti join boutiques on stores.`name` == boutiques.`boutiquename` """).show(false)
    println("END OF SparkSQL ANTI-JOIN")

    val bqDF = spark.sql("""SELECT * FROM boutiques""")
    val antiJoinDF = df.join(bqDF, df("name") === bqDF("boutiquename"), "anti")
    antiJoinDF.show(false)

    println("END OF SparkSQL ANTI-JOIN")
    val inOper = spark.sql("""SELECT * FROM stores WHERE stores.`name` IN (SELECT boutiquename FROM boutiques)""")
    inOper.show(false)
    println("END OF SparkSQL IN operator")
//    inOper.explain("formatted")

    val notInOper = spark.sql("""SELECT * FROM stores WHERE stores.`name` NOT IN (SELECT boutiquename FROM boutiques)""")
    notInOper.show(false)
    println("END OF SparkSQL NOT IN operator")

//    FULL JOIN
    val addStores = spark.createDataFrame(Seq(
      ("f",42,5,23),
      ("g", 19,7,18)
    )).toDF("name", "capacity", "opens", "closes")

    val fullJoined = df.union(addStores).join(occupancy,
      df("name") === occupancy("storename"),
      "full")

    fullJoined.show(false)
    println(" =" * 10 + " FULL JOIN " + "=" * 10)


    val unionByNameDF = df.unionByName(addStores, allowMissingColumns = true)
    unionByNameDF.show(false)

// TODO: TASK - Return names of stores with availability
    // Using INNER Query
    val capaDF1 = spark.sql(
      """ SELECT name, availability FROM (SELECT name, (capacity-occupants) AS availability
        FROM stores JOIN store_occupants
        ON stores.`name` == store_occupants.`storename`)
        WHERE availability> 4
        """.stripMargin)

    capaDF1.show(false)
    println("\n\tCapacity calculations by INNER QUERY.\n")


    // Using CONDITIONAL SELECT
    val capaDF2 = spark.sql(
      """ SELECT name, (capacity-occupants) AS availability
        FROM stores JOIN store_occupants
        ON stores.`name` == store_occupants.`storename`
        WHERE (capacity-occupants) > 4
        """.stripMargin)

    capaDF2.show(false)
    println("\n\tCapacity calculations by CONDITIONAL SELECT\n")



    // Using DataFrame Transformations
    import spark.implicits._

    val partySize = 4
    val hasSeats = df
      .join(occupancy, df("name") === occupancy("storename"))
      .withColumn("availability", $"capacity".minus($"occupants"))
      .where($"availability" >= partySize)
      .select("name", "availability")

    hasSeats.show(false)
  println("\n\tDataFrame Transformation - Seat availability\n")

  }
}