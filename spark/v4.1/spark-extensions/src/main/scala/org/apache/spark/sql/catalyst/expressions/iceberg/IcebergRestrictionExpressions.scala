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
package org.apache.spark.sql.catalyst.expressions.iceberg

import java.nio.ByteBuffer
import org.apache.iceberg.util.SerializableFunction
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Generic opaque masking wrapper. Applies a bound masking function from
 * [[org.apache.iceberg.restrictions.Actions]] to the child expression's value.
 *
 * Type bridging between Spark internal representation and Iceberg Java types:
 *  - String: UTF8String <-> String
 *  - Binary: byte[] <-> ByteBuffer
 *  - Decimal: Spark Decimal <-> BigDecimal
 *  - Int/Long/Float/Double/Boolean/Date/Timestamp: pass-through
 */
case class IcebergRestricted(child: Expression, mask: SerializableFunction[Object, Object])
    extends UnaryExpression
    with CodegenFallback {

  override def prettyName: String = "iceberg_restricted"
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val rawValue = child.eval(input)
    val icebergValue = if (rawValue == null) null else toIceberg(rawValue, child.dataType)
    val result = mask.apply(icebergValue.asInstanceOf[Object])
    if (result == null) null else fromIceberg(result, child.dataType)
  }

  private def toIceberg(value: Any, dt: DataType): Any = dt match {
    case StringType => value.asInstanceOf[UTF8String].toString
    case BinaryType => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
    case _: DecimalType => value.asInstanceOf[Decimal].toJavaBigDecimal
    case _ => value
  }

  private def fromIceberg(value: Any, dt: DataType): Any = dt match {
    case StringType => UTF8String.fromString(value.toString)
    case BinaryType =>
      val bb = value.asInstanceOf[ByteBuffer]
      val arr = new Array[Byte](bb.remaining())
      bb.duplicate().get(arr)
      arr
    case dt: DecimalType =>
      Decimal(value.asInstanceOf[java.math.BigDecimal], dt.precision, dt.scale)
    case _ => value
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/**
 * Opaque wrapper for the server-provided row filter. Hides the actual predicate
 * from EXPLAIN output so access-control policy details are not leaked.
 */
case class IcebergRowFilterExpr(child: Expression) extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override def prettyName: String = "iceberg_row_filter"

  override def sql: String = "iceberg_row_filter()"
  override def simpleString(maxFields: Int): String = "iceberg_row_filter()"
  override def toString: String = "iceberg_row_filter()"

  override protected def nullSafeEval(input: Any): Any = input

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
