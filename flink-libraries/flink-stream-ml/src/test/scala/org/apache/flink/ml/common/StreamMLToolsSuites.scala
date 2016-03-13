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

package org.apache.flink.ml.common

import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.stream.ml.common.StreamMLTools
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.testutils.FlinkStreamingTestBase
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class StreamMLToolsStreamingSuite extends FlatSpec with Matchers with FlinkStreamingTestBase {
  behavior of "StreamMLTools"

  it should "register the required streaming types" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    StreamMLTools.registerStreamMLTypes(env)

    val executionConfig = env.getConfig

    val serializer = new KryoSerializer[Nothing](classOf[Nothing], executionConfig)

    val kryo = serializer.getKryo()

    kryo.getRegistration(classOf[org.apache.flink.ml.math.DenseVector]).getId > 0 should be(true)
    kryo.getRegistration(classOf[org.apache.flink.ml.math.SparseVector]).getId > 0 should be(true)
    kryo.getRegistration(classOf[org.apache.flink.ml.math.DenseMatrix]).getId > 0 should be(true)
    kryo.getRegistration(classOf[org.apache.flink.ml.math.SparseMatrix]).getId > 0 should be(true)

    kryo.getRegistration(classOf[breeze.linalg.DenseMatrix[_]]).getId > 0 should be(true)
    kryo.getRegistration(classOf[breeze.linalg.CSCMatrix[_]]).getId > 0 should be(true)
    kryo.getRegistration(classOf[breeze.linalg.DenseVector[_]]).getId > 0 should be(true)
    kryo.getRegistration(classOf[breeze.linalg.SparseVector[_]]).getId > 0 should be(true)

    kryo.getRegistration(classOf[org.apache.flink.ml.common.WeightVector]).getId > 0 should be(true)
    kryo.getRegistration(classOf[org.apache.flink.ml.common.LabeledVector]).getId > 0 should be(true)

    kryo.getRegistration(breeze.linalg.DenseVector.zeros[Double](0).getClass).getId > 0 should
      be(true)
    kryo.getRegistration(breeze.linalg.SparseVector.zeros[Double](0).getClass).getId > 0 should
      be(true)
    kryo.getRegistration(breeze.linalg.DenseMatrix.zeros[Double](0, 0).getClass).getId > 0 should
      be(true)
    kryo.getRegistration(breeze.linalg.CSCMatrix.zeros[Double](0, 0).getClass).getId > 0 should
      be(true)
  }
}

class StreamMLToolsBatchSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "StreamMLTools"

  it should "register the required batch model types" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    StreamMLTools.registerFlinkMLModelTypes(env)

    val executionConfig = env.getConfig

    val serializer = new KryoSerializer[Nothing](classOf[Nothing], executionConfig)

    val kryo = serializer.getKryo()

    kryo.getRegistration(classOf[org.apache.flink.ml.common.WeightVector]).getId > 0 should be(true)
    kryo.getRegistration(classOf[org.apache.flink.ml.common.LabeledVector]).getId > 0 should be(true)
  }
}
