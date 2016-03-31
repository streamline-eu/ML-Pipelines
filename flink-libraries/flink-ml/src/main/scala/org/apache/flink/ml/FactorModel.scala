/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.api.common.operators.Order

object FactorModel {
  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    val inputFile = params.getRequired("input")
    val outputFile = params.getRequired("output")
    val topk = params.getInt("topk", 10000)


    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read and parse the input data
    val input = env.readTextFile(inputFile)
      .map(line => (line.split(" ")(1).toInt, line.split(" ")(2).toInt, 1.0))

    // Create a model using FlinkML
    val model = ALS()
      .setImplicit(true)

    model.fit(input)

    val users = input.map(_._1).distinct
    val items = input.map(_._2).distinct

    val test = users.cross(items)

    val output = model.predict(test)
      .groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .first(topk)

    output.writeAsText(outputFile, WriteMode.OVERWRITE)

    env execute
  }
}
