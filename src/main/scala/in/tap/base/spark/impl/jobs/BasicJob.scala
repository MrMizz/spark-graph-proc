package in.tap.base.spark.impl.jobs

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

case class BasicJob(inArgs: OneInArgs, outArgs: OneOutArgs)(
  implicit spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Basic],
  val writeTypeTagA: universe.TypeTag[BasicPlusOne]
) extends OneInOneOutJob[Basic, BasicPlusOne](inArgs, outArgs) {

  override def transform(input: Dataset[Basic]): Dataset[BasicPlusOne] = {
    input.map { basic: Basic =>
      BasicPlusOne(basic.data + 1)
    }
  }

}
