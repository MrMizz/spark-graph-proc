package in.tap.base.spark.impl.jobs

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

trait BaseSpec extends FlatSpec with Matchers {

  implicit val spark: SparkSession = {
    SparkSession.builder
      .appName("MySparkApp")
      .master("local[*]")
      .getOrCreate()
  }

}
