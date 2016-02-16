package org.apache.flink.ml

import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation.ALS

object FactorModel {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read and parse the input data
    val input = env.readCsvFile[(Int, Int)]("/tmp/flink-etl-out")
      .map(pair => (pair._1, pair._2, 1.0))

    // Create a model using FlinkML
    val model = ALS()
      .setImplicit(true)

    model.fit(input)

    val test = env.generateSequence(0,99).map(_.toInt) cross env.generateSequence(0,54).map(_.toInt)

//    val test = env.fromElements((0,0))

    model.predict(test).print
  }
}
