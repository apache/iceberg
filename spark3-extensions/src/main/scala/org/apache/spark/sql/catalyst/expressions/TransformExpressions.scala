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
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.transforms.Transforms
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

// TODO : Implement truncate expression.

abstract class IcebergTransformExpression
  extends Expression with CodegenFallback with NullIntolerant {

  def child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  @transient lazy val icebergInputType: Type = SparkSchemaUtil.convert(child.dataType)
}

abstract class IcebergTimeTransform
  extends IcebergTransformExpression with ImplicitCastInputTypes {

  def child: Expression
  def transform: Transform[Any, Integer]

  override def eval(input: InternalRow): Any = child.eval(input) match {
    case null =>
      null
    case value =>
      transform(value).toInt
  }

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
}

case class IcebergYearTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.year[Any](icebergInputType)
}

case class IcebergMonthTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.month[Any](icebergInputType)
}

case class IcebergDayTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.day[Any](icebergInputType)
}

case class IcebergHourTransform(child: Expression)
  extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.hour[Any](icebergInputType)
}

case class IcebergBucketTransform(numBuckets: Int, child: Expression) extends IcebergTransformExpression {

  override def children: Seq[Expression] = child :: Nil

  @transient lazy val bucketFunc: Any => Int = child.dataType match {
    case _: DecimalType =>
      val t = Transforms.bucket[Any](icebergInputType, numBuckets)
      d: Any => t(d.asInstanceOf[Decimal].toJavaBigDecimal).toInt
    case _: StringType =>
      // the spec requires that the hash of a string is equal to the hash of its UTF-8 encoded bytes
      // TODO: pass bytes without the copy out of the InternalRow
      val t = Transforms.bucket[ByteBuffer](Types.BinaryType.get(), numBuckets)
      s: Any => t(ByteBuffer.wrap(s.asInstanceOf[UTF8String].getBytes)).toInt
    case _ =>
      val t = Transforms.bucket[Any](icebergInputType, numBuckets)
      a: Any => t(a).toInt
  }

  override def eval(input: InternalRow): Any = child.eval(input) match {
    case null =>
      null
    case value =>
      bucketFunc(value)
  }

  override def dataType: DataType = IntegerType
}
