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
package org.apache.iceberg.spark.vendor

import org.apache.iceberg.io.FileIO
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.util.LongAccumulator
import scala.jdk.CollectionConverters._

case class BatchApplyExec(
    child: SparkPlan,
    function: String,
    functionClass: String,
    batchSize: Int,
    output: Seq[Attribute],
    io: FileIO,
    batches: LongAccumulator)
    extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def doExecute(): RDD[InternalRow] = {
    val inputTypes = child.output.map(_.dataType).toArray
    val outputTypes = output.map(_.dataType).toArray
    val size = batchSize
    val calls = batches
    val name = function
    val className = functionClass
    val fileIO = io

    child.execute().mapPartitions { rows =>
      val processor = FileBatchFunctions.instantiate(className)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => processor.close()))
      processor.initialize(fileIO)

      val toExternal = inputTypes.map(CatalystTypeConverters.createToScalaConverter)
      val toCatalyst = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      val unsafe = UnsafeProjection.create(outputTypes)

      rows
        // values are read out before buffering: the reader reuses the row it yields
        .map { row =>
          val values = new Array[Object](inputTypes.length)
          var i = 0
          while (i < values.length) {
            values(i) = toExternal(i)(row.get(i, inputTypes(i))).asInstanceOf[Object]
            i += 1
          }
          values
        }
        .grouped(size)
        .flatMap { batch =>
          calls.add(1L)
          val results = processor.apply(batch.asJava)
          require(
            results.size == batch.size,
            s"$name returned ${results.size} rows for a batch of ${batch.size}")

          results.asScala.iterator.map { values =>
            val converted = new Array[Any](outputTypes.length)
            var i = 0
            while (i < converted.length) {
              converted(i) = toCatalyst(i)(values(i))
              i += 1
            }
            unsafe(new GenericInternalRow(converted))
          }
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BatchApplyExec = {
    copy(child = newChild)
  }

  override def simpleString(maxFields: Int): String = {
    s"BatchApplyExec $function batchSize=$batchSize"
  }
}
