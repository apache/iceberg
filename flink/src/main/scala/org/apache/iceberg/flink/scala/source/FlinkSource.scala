/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.scala.source

import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.iceberg.flink.source.{FlinkSource => JavaFlinkSource}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.language.implicitConversions

object FlinkSource {

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}. See more options
   * in {@link ScanContext}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and read
   * records incrementally.
   * <ul>
   * <li>Without startSnapshotId: Bounded</li>
   * <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   * <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  def forRowData: JavaFlinkSource.Builder = new JavaFlinkSource.Builder

  def isBounded(properties: Map[String, String]): Boolean = JavaFlinkSource.isBounded(properties.asJava)

  class ScalaBuilder(builder: JavaFlinkSource.Builder) {
    def env(newEnv: StreamExecutionEnvironment): JavaFlinkSource.Builder = builder.env(newEnv.getJavaEnv)
  }

  implicit def javaBuild2ScalaBuild(builder: JavaFlinkSource.Builder): ScalaBuilder = new ScalaBuilder(builder)

  implicit def javaStream2ScalaStream[R](stream: JavaStream[R]): DataStream[R] = new DataStream[R](stream)
}
