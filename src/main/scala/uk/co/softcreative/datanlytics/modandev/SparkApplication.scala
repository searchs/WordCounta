package uk.co.softcreative.datanlytics.modandev

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import uk.co.softcreative.datanlytics.uk.co.softcreative.datanlytics.modandev.config.Configuration

trait SparkApplication extends App {
  val appName = Configuration.appName
  lazy val sparkConf: SparkConf = {

    val coreConf = new SparkConf()
      .setAppName(appName)

    //merge if missing
    Configuration.Spark.settings.foreach(tuple =>
      coreConf.setIfMissing(tuple._1, tuple._2))
    coreConf
  }


  lazy implicit val sparkSession: SparkSession = {
    SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  lazy val validationRules: Map[() => Boolean, String] = Map.empty
  def validateConfig()(implicit sparkSession: SparkSession):
  Boolean = {
    if (validationRules.nonEmpty) {
      val results = validationRules.foldLeft(List.empty[String])(
        (errs: List[String], rule: (() => Boolean, String)) => {
          if (rule._1()) {
            //continue to next rule
            errs
          } else {
            // if the predicate is not true, we hava problem
            errs :+ rule._2
          }
        })
      if (results.nonEmpty)
        throw new RuntimeException(s"Configuration Issues Encountered: \n ${results.mkString("\n")}")
      else true
    } else true
  }

  def run(): Unit = {
    validateConfig()
  }

}
