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

package org.apache.spark.sql.stats

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.SetOperationBuilder
import org.apache.datasketches.theta.Sketch
import org.apache.datasketches.theta.Sketches
import org.apache.datasketches.theta.UpdateSketch
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.SparkValueConverter
import org.apache.iceberg.types.Conversions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType

case class ThetaSketchAggregator(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[Sketch] with UnaryLike[Expression] {

  def this(child: Expression) = this(child, 0, 0)

  override def createAggregationBuffer(): Sketch = {
    UpdateSketch.builder.build
  }

  override def update(buffer: Sketch, input: InternalRow): Sketch = {
    val value = child.eval(input)
    if (value != null) {
      val icebergType = SparkSchemaUtil.convert(child.dataType)
      val byteBuffer = child.dataType match {
        case StringType => Conversions.toByteBuffer(icebergType,
          SparkValueConverter.convert(icebergType, value.toString))
        case _ => Conversions.toByteBuffer(icebergType, SparkValueConverter.convert(icebergType, value))
      }
      buffer.asInstanceOf[UpdateSketch].update(byteBuffer)
    }
    buffer
  }

  override def dataType: DataType = BinaryType

  override def merge(buffer: Sketch, input: Sketch): Sketch = {
    new SetOperationBuilder().buildUnion.union(buffer, input)
  }

  override def eval(buffer: Sketch): Any = {
    buffer.toByteArray
  }

  override def serialize(buffer: Sketch): Array[Byte] = {
    buffer.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): Sketch = {
    Sketches.wrapSketch(Memory.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
