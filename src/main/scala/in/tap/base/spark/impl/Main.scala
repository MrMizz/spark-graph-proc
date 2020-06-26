package in.tap.base.spark.impl

import in.tap.base.spark.impl.jobs.BasicJob
import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.main.{InArgs, OutArgs}
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "basic" => BasicJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _       => throw new MatchError("Invalid Step!")
    }
  }

}
