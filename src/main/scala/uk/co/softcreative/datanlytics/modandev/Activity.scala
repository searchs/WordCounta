package uk.co.softcreative.datanlytics.modandev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShopTransaction {
  case class Activity(
                       userId: String,
                       cartId: String,
                       itemId: String
                     )

  val activities = Seq(
    Activity("u1", "c1", "i1"),
    Activity("u1", "c1", "i2"),
    Activity("u2", "c2", "i1"),
    Activity("u3", "c3", "i3"),
    Activity("u4", "c4", "i3")
  )

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Spark Analytics example")
      .master("local[*]")
      .getOrCreate()

    val df = spark.createDataFrame(activities)
      .toDF("user_id", "cart_id", "item_id")


    df.show()

    // Daily active users
    val active_users = df.select("user_id").distinct.count()
    println(s"\n\tActive Users: ${active_users}")

    df.createOrReplaceTempView("activities")
    val act_users_sql = spark.sql(
      """
        select count(distinct(user_id)) as unique_users
        from activities
        """)

    act_users_sql.show(false)
    // Calculate the daily avg number of items across all user carts
    val avg_cart_items = df.select("cart_id", "item_id")
      .groupBy("cart_id")
      .agg(count(col("item_id")) as "total")
      .agg(avg(col("total")) as "avg_cart_items")

    avg_cart_items.show(false)
    avg_cart_items.explain()

    // Calculate the top 10 most added items across all user carts

    val top_items = df.select("item_id")
      .groupBy("item_id")
      .agg(count(col("item_id")) as "total")
      .sort(desc("total"))
      .limit(10)

    println("\n\t" + "==" * 45 + "\n")
    top_items.show(false)
    println("\n\t" + "**" * 45 + "\n")


  }

}




