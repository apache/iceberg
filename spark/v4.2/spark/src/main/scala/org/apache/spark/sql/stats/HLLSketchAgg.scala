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
import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.hll.Union
import org.apache.datasketches.memory.Memory
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
 * HLLSketchAgg generates HLL sketch with default seed and default lgK.
 * The values fed to the sketch are converted to bytes using Iceberg's single value serialization.
 * The result returned is an array of bytes of Compact HLL sketch of Datasketches library.
 *
 * See [[https://iceberg.apache.org/puffin-spec/]] for more information.
 */
case class HLLSketchAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[HllSketch]
    with UnaryLike[Expression] {

  private lazy val icebergType = SparkSchemaUtil.convert(child.dataType)

  def this(colName: String) = {
    this(analysis.UnresolvedAttribute.quotedString(colName), 0, 0)
  }

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): HllSketch = {
    new HllSketch()
  }

  override def update(buffer: HllSketch, input: InternalRow): HllSketch = {
    val value = child.eval(input)
    if (value != null) {
      val icebergValue = toIcebergValue(value)
      val byteBuffer = Conversions.toByteBuffer(icebergType, icebergValue)
      buffer.update(toBytes(byteBuffer))
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

  private def toBytes(byteBuffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](byteBuffer.remaining())
    byteBuffer.duplicate().get(bytes)
    bytes
  }

  override def merge(buffer: HllSketch, input: HllSketch): HllSketch = {
    val union = new Union(math.max(buffer.getLgConfigK, input.getLgConfigK))
    union.update(buffer)
    union.update(input)
    union.getResult
  }

  override def eval(buffer: HllSketch): Any = {
    buffer.toCompactByteArray
  }

  override def serialize(buffer: HllSketch): Array[Byte] = {
    buffer.toCompactByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): HllSketch = {
    HllSketch.heapify(Memory.wrap(storageFormat))
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
}
