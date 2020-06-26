package in.tap.base.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

trait BaseSpec extends FlatSpec with Matchers {

  final implicit val spark: SparkSession = {
    SparkSession.builder().appName("Spec").master("local[*]").getOrCreate()
  }

}
