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

package org.apache.spark.sql.catalyst.expressions

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets
import java.util.function
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.transforms.Transforms
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.ByteBuffers
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

abstract class IcebergTransformExpression
  extends UnaryExpression with CodegenFallback with NullIntolerant {

  @transient lazy val icebergInputType: Type = SparkSchemaUtil.convert(child.dataType)
}

abstract class IcebergTimeTransform
  extends IcebergTransformExpression with ImplicitCastInputTypes {

  def transform: function.Function[Any, Integer]

  override protected def nullSafeEval(value: Any): Any = {
    transform(value).toInt
  }

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
}

case class IcebergYearTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: function.Function[Any, Integer] = Transforms.year[Any]().bind(icebergInputType)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

case class IcebergMonthTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: function.Function[Any, Integer] = Transforms.month[Any]().bind(icebergInputType)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

case class IcebergDayTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: function.Function[Any, Integer] = Transforms.day[Any]().bind(icebergInputType)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

case class IcebergHourTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: function.Function[Any, Integer] = Transforms.hour[Any]().bind(icebergInputType)

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

case class IcebergBucketTransform(numBuckets: Int, child: Expression) extends IcebergTransformExpression {

  @transient lazy val bucketFunc: Any => Int = child.dataType match {
    case _: DecimalType =>
      val t = Transforms.bucket[Any](numBuckets).bind(icebergInputType)
      d: Any => t(d.asInstanceOf[Decimal].toJavaBigDecimal).toInt
    case _: StringType =>
      // the spec requires that the hash of a string is equal to the hash of its UTF-8 encoded bytes
      // TODO: pass bytes without the copy out of the InternalRow
      val t = Transforms.bucket[ByteBuffer](numBuckets).bind(Types.BinaryType.get())
      s: Any => t(ByteBuffer.wrap(s.asInstanceOf[UTF8String].getBytes)).toInt
    case _: BinaryType =>
      val t = Transforms.bucket[Any](numBuckets).bind(icebergInputType)
      b: Any => t(ByteBuffer.wrap(b.asInstanceOf[Array[Byte]])).toInt
    case _ =>
      val t = Transforms.bucket[Any](numBuckets).bind(icebergInputType)
      a: Any => t(a).toInt
  }

  override protected def nullSafeEval(value: Any): Any = {
    bucketFunc(value)
  }

  override def dataType: DataType = IntegerType

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

case class IcebergTruncateTransform(child: Expression, width: Int) extends IcebergTransformExpression {

  @transient lazy val truncateFunc: Any => Any = child.dataType match {
    case _: DecimalType =>
      val t = Transforms.truncate[java.math.BigDecimal](width).bind(icebergInputType)
      d: Any => Decimal.apply(t(d.asInstanceOf[Decimal].toJavaBigDecimal))
    case _: StringType =>
      val t = Transforms.truncate[CharSequence](width).bind(icebergInputType)
      s: Any => {
        val charSequence = t(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(s.asInstanceOf[UTF8String].getBytes)))
        val bb = StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence));
        UTF8String.fromBytes(ByteBuffers.toByteArray(bb))
      }
    case _: BinaryType =>
      val t = Transforms.truncate[ByteBuffer](width).bind(icebergInputType)
      s: Any => ByteBuffers.toByteArray(t(ByteBuffer.wrap(s.asInstanceOf[Array[Byte]])))
    case _ =>
      val t = Transforms.truncate[Any](width).bind(icebergInputType)
      a: Any => t(a)
  }

  override protected def nullSafeEval(value: Any): Any = {
    truncateFunc(value)
  }

  override def dataType: DataType = child.dataType

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
