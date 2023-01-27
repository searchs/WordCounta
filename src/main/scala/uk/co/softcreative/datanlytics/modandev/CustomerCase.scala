package uk.co.softcreative.datanlytics
package uk.co.softcreative.datanlytics.modandev

import org.apache.spark.sql._

import java.sql.Timestamp


object CustomerCase {
  case class Customer(
                       id: Integer,
                       created: Timestamp,
                       updated: Timestamp,
                       first_nae: String,
                       last_name: String,
                       email: String
                     )


  def main(args: Array[String]): Unit = {


    def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .appName("Spark Analytics")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val customersTable = spark.createDataFrame(Seq(
        ("1", "Scottie", "Raines", "scottie@coff.com"),
        ("2", "Johanna", "Bamm", "johanna.hamm@pacme.com"),
        ("3", "Tilo", "Baines", "tbaines@coff.com")
      )).toDF("id", "first_name", "last_name", "email")

      implicit val customerEnc = Encoders.product[Customer]
      val customerData: Dataset[Customer] = customersTable.as[Customer]

      customerData.filter(_.email.startsWith("johanna")).explain("formatted")


    }
  }

}