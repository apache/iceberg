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

import java.nio.ByteBuffer
import org.apache.datasketches.common.Family
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.CompactSketch
import org.apache.datasketches.theta.SetOperationBuilder
import org.apache.datasketches.theta.Sketch
import org.apache.datasketches.theta.UpdateSketch
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Conversions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * ThetaSketchAgg generates Alpha family sketch with default seed.
 * The values fed to the sketch are converted to bytes using Iceberg's single value serialization.
 * The result returned is an array of bytes of Compact Theta sketch of Datasketches library,
 * which should be deserialized to Compact sketch before using.
 *
 * See [[https://iceberg.apache.org/puffin-spec/]] for more information.
 */
case class ThetaSketchAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[Sketch]
    with UnaryLike[Expression] {

  private lazy val icebergType = SparkSchemaUtil.convert(child.dataType)

  def this(colName: String) = {
    this(analysis.UnresolvedAttribute.quotedString(colName), 0, 0)
  }

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): Sketch = {
    UpdateSketch.builder.setFamily(Family.ALPHA).build()
  }

  override def update(buffer: Sketch, input: InternalRow): Sketch = {
    val value = child.eval(input)
    if (value != null) {
      val icebergValue = toIcebergValue(value)
      val byteBuffer = Conversions.toByteBuffer(icebergType, icebergValue)
      buffer.asInstanceOf[UpdateSketch].update(byteBuffer)
    }
    buffer
  }

  private def toIcebergValue(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case d: Decimal => d.toJavaBigDecimal
      case b: Array[Byte] => ByteBuffer.wrap(b)
      case _ => value
    }
  }

  override def merge(buffer: Sketch, input: Sketch): Sketch = {
    new SetOperationBuilder().buildUnion.union(buffer, input)
  }

  override def eval(buffer: Sketch): Any = {
    toBytes(buffer)
  }

  override def serialize(buffer: Sketch): Array[Byte] = {
    toBytes(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): Sketch = {
    CompactSketch.wrap(Memory.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  private def toBytes(sketch: Sketch): Array[Byte] = {
    val compactSketch = sketch.compact()
    compactSketch.toByteArray
  }
}
