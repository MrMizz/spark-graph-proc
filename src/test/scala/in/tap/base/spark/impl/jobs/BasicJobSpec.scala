package in.tap.base.spark.impl.jobs

import in.tap.base.spark.io.{In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Encoder, Encoders}

class BasicJobSpec extends BaseSpec {

  it should "add one" in {

    val dir: String = {
      getClass.getResource("/basic/in/basic.json").getPath.replace("in/basic.json", "")
    }

    val inArgs: OneInArgs = OneInArgs(
      in1 = In(s"$dir/in/")
    )

    val outArgs: OneOutArgs = OneOutArgs(
      out1 = Out(s"$dir/out/")
    )

    BasicJob(inArgs, outArgs).execute()

    val results: Seq[BasicPlusOne] = {
      import spark.implicits._
      implicit val _: Encoder[BasicPlusOne] = Encoders.product[BasicPlusOne]
      spark.read.json(outArgs.out1.path).as[BasicPlusOne].collect.toSeq
    }

    results.map(_.data).sum shouldBe 20
  }

}
